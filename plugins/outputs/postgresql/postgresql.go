package postgresql

import (
	"fmt"
	"log"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/columns"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/db"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/tables"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/utils"
)

type Postgresql struct {
	Connection                  string
	Schema                      string
	DoSchemaUpdates             bool
	TagsAsForeignkeys           bool
	CachedTagsetsPerMeasurement int
	TagsAsJsonb                 bool
	FieldsAsJsonb               bool
	TableTemplate               string
	TagTableSuffix              string

	db         db.Wrapper
	tables     tables.Manager
	tagCache   tagsCache

	rows    transformer
	columns columns.Mapper
}

func init() {
	outputs.Add("postgresql", func() telegraf.Output { return newPostgresql() })
}

func newPostgresql() *Postgresql {
	return &Postgresql{
		Schema:                      "public",
		TableTemplate:               "CREATE TABLE IF NOT EXISTS {TABLE}({COLUMNS})",
		TagTableSuffix:              "_tag",
		CachedTagsetsPerMeasurement: 1000,
		DoSchemaUpdates:             true,
	}
}

// Connect establishes a connection to the target database and prepares the cache
func (p *Postgresql) Connect() error {
	// set p.db with a lock
	dbWrapper, err := db.NewWrapper(p.Connection)
	if err != nil {
		return err
	}
	p.db = dbWrapper
	p.tables = tables.NewManager(p.Schema, p.TableTemplate)

	if p.TagsAsForeignkeys {
		p.tagCache = newTagsCache(p.CachedTagsetsPerMeasurement, p.TagsAsJsonb, p.TagTableSuffix, p.Schema)
	}
	p.rows = newRowTransformer(p.TagsAsForeignkeys, p.TagsAsJsonb, p.FieldsAsJsonb, p.tagCache)
	p.columns = columns.NewMapper(p.TagsAsForeignkeys, p.TagsAsJsonb, p.FieldsAsJsonb)
	return nil
}

// Close closes the connection to the database
func (p *Postgresql) Close() error {
	p.tagCache = nil
	p.tables = nil
	return p.db.Close()
}

var sampleConfig = `
  ## specify address via a url matching:
  ##   postgres://[pqgotest[:password]]@localhost[/dbname]\
  ##       ?sslmode=[disable|verify-ca|verify-full]
  ## or a simple string:
  ##   host=localhost user=pqotest password=... sslmode=... dbname=app_production
  ##
  ## All connection parameters are optional. Also supported are PG environment vars
  ## e.g. PGPASSWORD, PGHOST, PGUSER, PGDATABASE 
  ## all supported vars here: https://www.postgresql.org/docs/current/libpq-envars.html
  ##
  ## Without the dbname parameter, the driver will default to a database
  ## with the same name as the user. This dbname is just for instantiating a
  ## connection with the server and doesn't restrict the databases we are trying
  ## to grab metrics for.
  ##
  connection = "host=localhost user=postgres sslmode=verify-full"

  ## Update existing tables to match the incoming metrics automatically. Default is true
  # do_schema_updates = true

  ## Store tags as foreign keys in the metrics table. Default is false.
  # tags_as_foreignkeys = false
  
  ## If tags_as_foreignkeys is set to true you can choose the number of tag sets to cache
  ## per measurement (metric name). Default is 1000, if set to 0 => cache has no limit.
  ## Has no effect if tags_as_foreignkeys = false
  # cached_tagsets_per_measurement = 1000

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
  # tags_as_jsonb = false

  ## Use jsonb datatype for fields
  # fields_as_jsonb = false

`

func (p *Postgresql) SampleConfig() string { return sampleConfig }
func (p *Postgresql) Description() string  { return "Send metrics to PostgreSQL" }

func (p *Postgresql) Write(metrics []telegraf.Metric) error {
	if !p.checkConnection() {
		log.Println("W! Connection is not alive, attempting reset")
		if err := p.resetConnection(); err != nil {
			return err
		}
		log.Println("I! Connection established again")
	}
	metricsByMeasurement := utils.GroupMetricsByMeasurement(metrics)
	for measureName, groupedMetrics := range metricsByMeasurement {
		err := p.writeMetricsFromMeasure(measureName, groupedMetrics)
		if err != nil {
			return err
		}
	}
	return nil
}

// Writes only the metrics from a specified measure. 'metricIndices' is an array
// of the metrics that belong to the selected 'measureName' for faster lookup.
// If schema updates are enabled the target db tables are updated to be able
// to hold the new values.
func (p *Postgresql) writeMetricsFromMeasure(measureName string, metrics []telegraf.Metric) error {
	targetColumns, targetTagColumns := p.columns.Target(metrics)

	if p.DoSchemaUpdates {
		tagTable := true
		if err := p.prepareTable(measureName, targetColumns, !tagTable); err != nil {
			return err
		}
		if p.TagsAsForeignkeys {
			tagTableName := p.tagCache.tagsTableName(measureName)
			if err := p.prepareTable(tagTableName, targetTagColumns, tagTable); err != nil {
				return err
			}
		}
	}
	numColumns := len(targetColumns.Names)
	values := make([][]interface{}, len(metrics))
	var rowTransformErr error
	for rowNum, metric := range metrics {
		values[rowNum], rowTransformErr = p.rows.createRowFromMetric(p.db, numColumns, metric, targetColumns, targetTagColumns)
		if rowTransformErr != nil {
			return fmt.Errorf("E! Could not transform metric to proper row\n%v", rowTransformErr)
		}
	}

	fullTableName := utils.FullTableName(p.Schema, measureName)
	return p.db.DoCopy(fullTableName, targetColumns.Names, values)
}

// Checks if a table exists in the db, and then validates if all the required columns
// are present or some are missing (if metrics changed their field or tag sets).
func (p *Postgresql) prepareTable(tableName string, details *utils.TargetColumns, tagTable bool) error {
	tableExists := p.tables.Exists(p.db,tableName)

	if !tableExists {
		return p.tables.CreateTable(p.db, tableName, details, tagTable)
	}

	missingColumns, err := p.tables.FindColumnMismatch(p.db,tableName, details)
	if err != nil {
		return err
	}
	if len(missingColumns) == 0 {
		return nil
	}
	return p.tables.AddColumnsToTable(p.db,tableName, missingColumns, details)
}

func (p *Postgresql) checkConnection() bool {
	return p.db != nil && p.db.IsAlive()
}

func (p *Postgresql) resetConnection() error {
	var err error
	p.db, err = db.NewWrapper(p.Connection)
	if err != nil {
		return fmt.Errorf("E! Could not reset connection:\n%v", err)
	}
	return nil
}
