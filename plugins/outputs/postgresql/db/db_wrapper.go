package db

import (
	"log"

	"github.com/jackc/pgx"
	// pgx driver for sql connections
	_ "github.com/jackc/pgx/stdlib"
)

const checkConnQuery = "SELECT 1"

// Wrapper defines an interface that encapsulates communication with a DB.
type Wrapper interface {
	Close() error
	Begin() (*pgx.Tx, error)
	IsAlive() bool
}

type defaultDbWrapper struct {
	db *pgx.Conn
}

// NewWrapper returns an implementation of the db.Wrapper interface
// that issues queries to a PG database.
func NewWrapper(connection string) (Wrapper, error) {
	connConfig, err := parseConnectionString(connection)
	connConfig.PreferSimpleProtocol = true
	if err != nil {
		return nil, err
	}
	db, err := pgx.Connect(*connConfig)
	if err != nil {
		log.Printf("E! Couldn't connect to server\n%v", err)
		return nil, err
	}

	return &defaultDbWrapper{
		db: db,
	}, nil
}

func (d *defaultDbWrapper) Begin() (*pgx.Tx, error) {
	return d.db.Begin()
}
func (d *defaultDbWrapper) Close() error { return d.db.Close() }

func (d *defaultDbWrapper) Query(tx *pgx.Tx, query string, args ...interface{}) (*pgx.Rows, error) {
	return tx.Query(query, args...)
}

func (d *defaultDbWrapper) IsAlive() bool {
	if !d.db.IsAlive() {
		return false
	}
	row := d.db.QueryRow(checkConnQuery)
	var one int64
	if err := row.Scan(&one); err != nil {
		log.Printf("W! Error given on 'is conn alive':\n%v", err)
		return false
	}
	return true
}

func parseConnectionString(connection string) (*pgx.ConnConfig, error) {
	envConnConfig, err := pgx.ParseEnvLibpq()
	if err != nil {
		log.Println("E! couldn't check PG environment variables")
		return nil, err
	}

	connConfig, err := pgx.ParseConnectionString(connection)
	if err != nil {
		log.Printf("E! Couldn't parse connection string: %s\n%v", connection, err)
		return nil, err
	}

	connConfig = envConnConfig.Merge(connConfig)
	return &connConfig, nil
}
