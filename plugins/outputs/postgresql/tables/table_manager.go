package tables

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/influxdata/telegraf/plugins/outputs/postgresql/columns"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/db"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/utils"
)

const (
	tagTableSQLTemplate        = `CREATE TABLE IF NOT EXISTS {TABLE}({COLUMNS}, PRIMARY KEY("` + columns.TagIDColumnName + `"))`
	addColumnTemplate          = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;"
	tableExistsTemplate        = "SELECT tablename FROM pg_tables WHERE tablename = $1 AND schemaname = $2;"
	findColumnPresenceTemplate = "WITH available AS (SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 and table_name = $2)," +
		"required AS (SELECT c FROM unnest(array [%s]) AS c) " +
		"SELECT required.c as column_name, available.column_name IS NOT NULL as exists, available.data_type FROM required LEFT JOIN available ON required.c = available.column_name;"
)

type columnInDbDef struct {
	dataType utils.PgDataType
	exists   bool
}

func (c *columnInDbDef) String() string {
	return fmt.Sprintf("{dType: %s, exists:%v}", c.dataType, c.exists)
}

// Manager defines an abstraction that can check the state of tables in a PG
// database, create, and update them.
type Manager interface {
	// Exists checks if a table with the given name already is present in the DB.
	Exists(tableName string) bool
	// Creates a table in the database with the column names and types specified in 'colDetails'
	CreateTable(tableName string, colDetails *utils.TargetColumns, tagTable bool) error
	// This function queries a table in the DB if the required columns in 'colDetails' are present and what is their
	// data type. For existing columns it checks if the data type in the DB can safely hold the data from the metrics.
	// It returns:
	//   - the indices of the missing columns (from colDetails)
	//   - or an error if
	//     = it couldn't discover the columns of the table in the db
	//     = the existing column types are incompatible with the required column types
	FindColumnMismatch(tableName string, colDetails *utils.TargetColumns) ([]int, error)
	// From the column details (colDetails) of a given measurement, 'columnIndices' specifies which are missing in the DB.
	// this function will add the new columns with the required data type.
	AddColumnsToTable(tableName string, columnIndices []int, colDetails *utils.TargetColumns) error
	SetConnection(db db.Wrapper)
}

type defTableManager struct {
	Tables        map[string]bool
	db            db.Wrapper
	schema        string
	tableTemplate string
}

// NewManager returns an instance of the tables.Manager interface
// that can handle checking and updating the state of tables in the PG database.
func NewManager(db db.Wrapper, schema, tableTemplate string) Manager {
	return &defTableManager{
		Tables:        make(map[string]bool),
		db:            db,
		tableTemplate: tableTemplate,
		schema:        schema,
	}
}

// SetConnection to db, used only when previous was killed or restarted.
// It will also clear the local cache of which table exists.
func (t *defTableManager) SetConnection(db db.Wrapper) {
	t.db = db
	t.Tables = make(map[string]bool)
}

// Exists checks if a table with the given name already is present in the DB.
func (t *defTableManager) Exists(tableName string) bool {
	if _, ok := t.Tables[tableName]; ok {
		return true
	}

	commandTag, err := t.db.Exec(tableExistsTemplate, tableName, t.schema)
	if err != nil {
		log.Printf("E! Error checking for existence of metric table: %s\nSQL: %s\n%v", tableName, tableExistsTemplate, err)
		return false
	}

	if commandTag.RowsAffected() == 1 {
		t.Tables[tableName] = true
		return true
	}

	return false
}

// Creates a table in the database with the column names and types specified in 'colDetails'
func (t *defTableManager) CreateTable(tableName string, colDetails *utils.TargetColumns, tagsTable bool) error {
	var sql string
	if tagsTable {
		sql = t.generateCreateTagTableSQL(tableName, colDetails)
	} else {
		sql = t.generateCreateTableSQL(tableName, colDetails)
	}
	if _, err := t.db.Exec(sql); err != nil {
		log.Printf("E! Couldn't create table: %s\nSQL: %s\n%v", tableName, sql, err)
		return err
	}

	t.Tables[tableName] = true
	return nil
}

// This function queries a table in the DB if the required columns in 'colDetails' are present and what is their
// data type. For existing columns it checks if the data type in the DB can safely hold the data from the metrics.
// It returns:
//   - the indices of the missing columns (from colDetails)
//   - or an error if
//     = it couldn't discover the columns of the table in the db
//     = the existing column types are incompatible with the required column types
func (t *defTableManager) FindColumnMismatch(tableName string, colDetails *utils.TargetColumns) ([]int, error) {
	if tableName == "" || colDetails == nil || colDetails.Names == nil || len(colDetails.Names) == 0 {
		errStr := fmt.Sprintf("attempted to find column missmatch for table '%s' with column details: %v", tableName, colDetails)
		log.Println("E! " + errStr)
		return nil, errors.New(errStr)
	}

	columnPresence, err := t.findColumnPresence(tableName, colDetails.Names)
	if err != nil {
		return nil, err
	} else if columnPresence == nil || len(columnPresence) != len(colDetails.Names) {
		errStr := fmt.Sprintf("presence not discovered for all columns (%v) of table '%s'; discovered only: %v", colDetails.Names, tableName, columnPresence)
		log.Println("E! " + errStr)
		return nil, errors.New(errStr)
	}
	missingCols := []int{}
	for colIndex := range colDetails.Names {
		colStateInDb := columnPresence[colIndex]
		if !colStateInDb.exists {
			missingCols = append(missingCols, colIndex)
			continue
		}
		typeInDb := colStateInDb.dataType
		typeInMetric := colDetails.DataTypes[colIndex]
		if !utils.PgTypeCanContain(typeInDb, typeInMetric) {
			errStr := fmt.Sprintf("A column exists in '%s' of type '%s' required type '%s'", tableName, typeInDb, typeInMetric)
			log.Println("E! " + errStr)
			return nil, errors.New(errStr)
		}
	}

	return missingCols, nil
}

// From the column details (colDetails) of a given measurement, 'columnIndices' specifies which are missing in the DB.
// this function will add the new columns with the required data type.
func (t *defTableManager) AddColumnsToTable(tableName string, columnIndices []int, colDetails *utils.TargetColumns) error {
	if tableName == "" || columnIndices == nil || colDetails == nil || len(columnIndices) == 0 {
		errStr := fmt.Sprintf("attempted to add new columns to table '%s'. indices: %v, details: %v", tableName, columnIndices, colDetails)
		log.Println("E! " + errStr)
		return fmt.Errorf(errStr)
	}
	fullTableName := utils.FullTableName(t.schema, tableName).Sanitize()
	for _, colIndex := range columnIndices {
		name := colDetails.Names[colIndex]
		dataType := colDetails.DataTypes[colIndex]
		addColumnQuery := fmt.Sprintf(addColumnTemplate, fullTableName, utils.QuoteIdent(name), dataType)
		_, err := t.db.Exec(addColumnQuery)
		if err != nil {
			log.Printf("E! Couldn't add missing columns to the table: %s\nError executing: %s\n%v", tableName, addColumnQuery, err)
			return err
		}
	}

	return nil
}

// Populate the 'tableTemplate' (supplied as config option to the plugin) with the details of
// the required columns for the measurement to create a 'CREATE TABLE' SQL statement.
// The order, column names and data types are given in 'colDetails'.
func (t *defTableManager) generateCreateTableSQL(tableName string, colDetails *utils.TargetColumns) string {
	colDefs := make([]string, len(colDetails.Names))
	pk := []string{}
	for colIndex, colName := range colDetails.Names {
		colDefs[colIndex] = utils.QuoteIdent(colName) + " " + string(colDetails.DataTypes[colIndex])
		if colDetails.Roles[colIndex] != utils.FieldColType {
			pk = append(pk, colName)
		}
	}

	fullTableName := utils.FullTableName(t.schema, tableName).Sanitize()
	query := strings.Replace(t.tableTemplate, "{TABLE}", fullTableName, -1)
	query = strings.Replace(query, "{TABLELITERAL}", utils.QuoteLiteral(fullTableName), -1)
	query = strings.Replace(query, "{COLUMNS}", strings.Join(colDefs, ","), -1)
	query = strings.Replace(query, "{KEY_COLUMNS}", strings.Join(pk, ","), -1)

	return query
}

func (t *defTableManager) generateCreateTagTableSQL(tableName string, colDetails *utils.TargetColumns) string {
	colDefs := make([]string, len(colDetails.Names))
	pk := []string{}
	for colIndex, colName := range colDetails.Names {
		colDefs[colIndex] = utils.QuoteIdent(colName) + " " + string(colDetails.DataTypes[colIndex])
		if colDetails.Roles[colIndex] != utils.FieldColType {
			pk = append(pk, colName)
		}
	}

	fullTableName := utils.FullTableName(t.schema, tableName).Sanitize()
	query := strings.Replace(tagTableSQLTemplate, "{TABLE}", fullTableName, -1)
	query = strings.Replace(query, "{COLUMNS}", strings.Join(colDefs, ","), -1)

	return query
}

// For a given table and an array of column names it checks the database if those columns exist,
// and what's their data type.
func (t *defTableManager) findColumnPresence(tableName string, columns []string) ([]*columnInDbDef, error) {
	if tableName == "" || columns == nil || len(columns) == 0 {
		errStr := fmt.Sprintf("attempted to find the presence of columns %v in table '%s'; something is not right", columns, tableName)
		log.Println("E! " + errStr)
		return nil, errors.New(errStr)
	}
	columnPresenseQuery := prepareColumnPresenceQuery(columns)
	result, err := t.db.Query(columnPresenseQuery, t.schema, tableName)
	if err != nil {
		log.Printf("E! Couldn't discover columns of table: %s\nQuery failed: %s\n%v", tableName, columnPresenseQuery, err)
		return nil, err
	}
	defer result.Close()
	columnStatus := make([]*columnInDbDef, len(columns))
	var exists bool
	var columnName string
	var pgLongType sql.NullString
	currentColumn := 0

	for result.Next() {
		err := result.Scan(&columnName, &exists, &pgLongType)
		if err != nil {
			log.Printf("E! Couldn't discover columns of table: %s\n%v", tableName, err)
			return nil, err
		}
		pgShortType := utils.PgDataType("")
		if pgLongType.Valid {
			pgShortType = utils.LongToShortPgType(pgLongType.String)
		}
		columnStatus[currentColumn] = &columnInDbDef{
			exists:   exists,
			dataType: pgShortType,
		}
		currentColumn++
	}

	return columnStatus, nil
}

func prepareColumnPresenceQuery(columns []string) string {
	quotedColumns := make([]string, len(columns))
	for i, column := range columns {
		quotedColumns[i] = utils.QuoteLiteral(column)
	}
	return fmt.Sprintf(findColumnPresenceTemplate, strings.Join(quotedColumns, ","))
}
