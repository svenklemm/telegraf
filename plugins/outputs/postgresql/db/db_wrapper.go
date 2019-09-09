package db

import (
	"fmt"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/utils"

	"github.com/jackc/pgx"
	// pgx driver for sql connections
	_ "github.com/jackc/pgx/stdlib"
)

const checkConnQuery = "SELECT 1"

// Wrapper defines an interface that encapsulates communication with a DB.
type Wrapper interface {
	Exec(query string, args ...interface{}) (pgx.CommandTag, error)
	DoCopy(fullTableName *pgx.Identifier, colNames []string, batch [][]interface{}) *utils.ErrorBundle
	Query(query string, args ...interface{}) (*pgx.Rows, error)
	QueryRow(query string, args ...interface{}) *pgx.Row
	Close() error
	IsAlive() bool
}

type defaultDbWrapper struct {
	db *pgx.Conn
}

// NewWrapper returns an implementation of the db.Wrapper interface
// that issues queries to a PG database.
func NewWrapper(connection string) (Wrapper, error) {
	connConfig, err := parseConnectionString(connection)
	if err != nil {
		return nil, err
	}
	db, err := pgx.Connect(*connConfig)
	if err != nil {
		return nil, fmt.Errorf("E! Couldn't connect to server\n%v", err)
	}

	return &defaultDbWrapper{
		db: db,
	}, nil
}

func (d *defaultDbWrapper) Exec(query string, args ...interface{}) (pgx.CommandTag, error) {
	return d.db.Exec(query, args...)
}

func (d *defaultDbWrapper) DoCopy(
	fullTableName *pgx.Identifier,
	colNames []string,
	batch [][]interface{}) *utils.ErrorBundle {
	source := pgx.CopyFromRows(batch)
	_, err := d.db.CopyFrom(*fullTableName, colNames, source)
	if err != nil {
		return utils.DecodePgError(err)
	}

	return nil
}

func (d *defaultDbWrapper) Close() error { return d.db.Close() }

func (d *defaultDbWrapper) Query(query string, args ...interface{}) (*pgx.Rows, error) {
	return d.db.Query(query, args...)
}

func (d *defaultDbWrapper) QueryRow(query string, args ...interface{}) *pgx.Row {
	return d.db.QueryRow(query, args...)
}

func (d *defaultDbWrapper) IsAlive() bool {
	if !d.db.IsAlive() {
		return false
	}
	row := d.db.QueryRow(checkConnQuery)
	var one int64
	if err := row.Scan(&one); err != nil {
		return false
	}
	return true
}

func parseConnectionString(connection string) (*pgx.ConnConfig, error) {
	envConnConfig, err := pgx.ParseEnvLibpq()
	if err != nil {
		return nil, fmt.Errorf("E! Couldn't check PG environment variables\n%v", err)
	}

	connConfig, err := pgx.ParseConnectionString(connection)
	if err != nil {
		return nil, fmt.Errorf("E! Couldn't parse connection string: %s\n%v", connection, err)
	}

	connConfig = envConnConfig.Merge(connConfig)
	return &connConfig, nil
}
