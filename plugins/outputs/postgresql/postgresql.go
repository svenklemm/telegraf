package postgresql

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

type Postgresql struct {
	db                *sql.DB
	Address           string
	Schema            string
	TagsAsForeignkeys bool
	TagsAsJsonb       bool
	FieldsAsJsonb     bool
	TableTemplate     string
	TagTableSuffix    string
	tables            *tableKeeper
}

func init() {
	outputs.Add("postgresql", func() telegraf.Output { return newPostgresql() })
}

func newPostgresql() *Postgresql {
	return &Postgresql{
		Schema:         "public",
		TableTemplate:  "CREATE TABLE IF NOT EXISTS {TABLE}({COLUMNS})",
		TagsAsJsonb:    true,
		TagTableSuffix: "_tag",
		FieldsAsJsonb:  true,
	}
}

func (p *Postgresql) Connect() error {
	db, err := sql.Open("pgx", p.Address)
	if err != nil {
		return err
	}
	p.db = db
	p.tables = newTableKeeper(db)
	return nil
}

func (p *Postgresql) Close() error {
	return p.db.Close()
}

func (p *Postgresql) fullTableName(name string) string {
	return quoteIdent(p.Schema) + "." + quoteIdent(name)
}

var sampleConfig = `
  ## specify address via a url matching:
  ##   postgres://[pqgotest[:password]]@localhost[/dbname]\
  ##       ?sslmode=[disable|verify-ca|verify-full]
  ## or a simple string:
  ##   host=localhost user=pqotest password=... sslmode=... dbname=app_production
  ##
  ## All connection parameters are optional.
  ##
  ## Without the dbname parameter, the driver will default to a database
  ## with the same name as the user. This dbname is just for instantiating a
  ## connection with the server and doesn't restrict the databases we are trying
  ## to grab metrics for.
  ##
  address = "host=localhost user=postgres sslmode=verify-full"

  ## Store tags as foreign keys in the metrics table. Default is false.
  # tags_as_foreignkeys = false

  ## Template to use for generating tables
  ## Available Variables:
  ##   {TABLE} - tablename as identifier
  ##   {TABLELITERAL} - tablename as string literal
  ##   {COLUMNS} - column definitions
  ##   {KEY_COLUMNS} - comma-separated list of key columns (time + tags)

  ## Default template
  # table_template = "CREATE TABLE IF NOT EXISTS {TABLE}({COLUMNS})"
  ## Example for timescaledb
  # table_template = "CREATE TABLE IF NOT EXISTS {TABLE}({COLUMNS}); SELECT create_hypertable({TABLELITERAL},'time',chunk_time_interval := '1 week'::interval,if_not_exists := true);"

  ## Schema to create the tables into
  # schema = "public"

  ## Use jsonb datatype for tags
  # tags_as_jsonb = true

  ## Use jsonb datatype for fields
  # fields_as_jsonb = true

`

func (p *Postgresql) SampleConfig() string { return sampleConfig }
func (p *Postgresql) Description() string  { return "Send metrics to PostgreSQL" }

func (p *Postgresql) Write(metrics []telegraf.Metric) error {
	batches := make(map[string][]interface{})
	params := make(map[string][]string)
	colmap := make(map[string][]string)
	tabmap := make(map[string]string)

	for _, metric := range metrics {
		tablename := metric.Name()

		// create table if needed
		if p.tables.tableExists(p.Schema, tablename) == false {
			createStmt := p.generateCreateTable(metric)
			_, err := p.db.Exec(createStmt)
			if err != nil {
				log.Printf("E! Creating table failed: statement: %v, error: %v", createStmt, err)
				return err
			}
			p.tables.addTable(tablename)
		}

		columns := []string{"time"}
		values := []interface{}{metric.Time()}

		if len(metric.Tags()) > 0 {
			if p.TagsAsForeignkeys {
				// tags in separate table
				tagID, err := p.getTagID(metric)
				if err != nil {
					return err
				}
				columns = append(columns, "tag_id")
				values = append(values, tagID)
			} else {
				// tags in measurement table
				if p.TagsAsJsonb {
					d, err := buildJsonbTags(metric.Tags())
					if err != nil {
						return err
					}

					if d != nil {
						columns = append(columns, "tags")
						values = append(values, d)
					}
				} else {
					var keys []string
					fields := metric.Tags()
					for column := range fields {
						keys = append(keys, column)
					}
					sort.Strings(keys)
					for _, column := range keys {
						columns = append(columns, column)
						values = append(values, fields[column])
					}
				}
			}
		}

		if p.FieldsAsJsonb {
			d, err := buildJsonb(metric.Fields())
			if err != nil {
				return err
			}

			columns = append(columns, "fields")
			values = append(values, d)
		} else {
			var keys []string
			fields := metric.Fields()
			for column := range fields {
				keys = append(keys, column)
			}
			sort.Strings(keys)
			for _, column := range keys {
				columns = append(columns, column)
				values = append(values, fields[column])
			}
		}

		var tableAndCols string
		var placeholder, quotedColumns []string
		for _, column := range columns {
			quotedColumns = append(quotedColumns, quoteIdent(column))
		}
		tableAndCols = fmt.Sprintf("%s(%s)", p.fullTableName(tablename), strings.Join(quotedColumns, ","))
		batches[tableAndCols] = append(batches[tableAndCols], values...)
		for i := range columns {
			i += len(params[tableAndCols]) * len(columns)
			placeholder = append(placeholder, fmt.Sprintf("$%d", i+1))
		}
		params[tableAndCols] = append(params[tableAndCols], strings.Join(placeholder, ","))
		colmap[tableAndCols] = columns
		tabmap[tableAndCols] = tablename
	}

	return p.insertBatches(batches, tabmap, colmap, params)
}

func (p *Postgresql) insertBatches(
	batches map[string][]interface{},
	tabmap map[string]string,
	colmap, params map[string][]string) error {
	for tableAndCols, values := range batches {
		sql := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableAndCols, strings.Join(params[tableAndCols], "),("))
		_, err := p.db.Exec(sql, values...)
		if err != nil {
			// check if insert error was caused by column mismatch
			retry := false
			if p.FieldsAsJsonb == false {
				log.Printf("E! Error during insert: %v", err)
				tablename := tabmap[tableAndCols]
				columns := colmap[tableAndCols]
				var quotedColumns []string
				for _, column := range columns {
					quotedColumns = append(quotedColumns, quoteLiteral(column))
				}
				query := "SELECT c FROM unnest(array[%s]) AS c WHERE NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE column_name=c AND table_schema=$1 AND table_name=$2)"
				query = fmt.Sprintf(query, strings.Join(quotedColumns, ","))
				result, err := p.db.Query(query, p.Schema, tablename)
				if err != nil {
					return err
				}
				defer result.Close()

				// some columns are missing
				var column, datatype string
				for result.Next() {
					err := result.Scan(&column)
					if err != nil {
						log.Println(err)
					}
					for i, name := range columns {
						if name == column {
							datatype = deriveDatatype(values[i])
						}
					}
					query := "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;"
					_, err = p.db.Exec(fmt.Sprintf(query, p.fullTableName(tablename), quoteIdent(column), datatype))
					if err != nil {
						return err
					}
					retry = true
				}
			}

			// We added some columns and insert might work now. Try again immediately to
			// avoid long lead time in getting metrics when there are several columns missing
			// from the original create statement and they get added in small drops.
			if retry {
				_, err = p.db.Exec(sql, values...)
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}
