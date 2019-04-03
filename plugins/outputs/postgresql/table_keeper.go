package postgresql

import (
	"database/sql"
	"log"
)

const (
	tableExistsTemplate = "SELECT tablename FROM pg_tables WHERE tablename = $1 AND schemaname = $2;"
)

type tableKeeper struct {
	Tables map[string]bool
	db     *sql.DB
}

func newTableKeeper(db *sql.DB) *tableKeeper {
	return &tableKeeper{
		Tables: make(map[string]bool),
	}
}

func (t *tableKeeper) tableExists(schema, tableName string) bool {
	if _, ok := t.Tables[tableName]; ok {
		return true
	}

	result, err := t.db.Exec(tableExistsTemplate, tableName, schema)
	if err != nil {
		log.Printf("E! Error checking for existence of metric table %s: %v", tableName, err)
		return false
	}
	if count, _ := result.RowsAffected(); count == 1 {
		t.Tables[tableName] = true
		return true
	}
	return false
}

func (t *tableKeeper) addTable(tableName string) {
	t.Tables[tableName] = true
}
