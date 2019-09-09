package db

import (
	"fmt"
	"github.com/jackc/pgx"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseConnectionStringPgEnvOverride(t *testing.T) {
	config, err := parseConnectionString("dbname=test")
	assert.NoError(t, err)
	assert.Equal(t, "test", config.Database)
	assert.Equal(t, "", config.Password)

	os.Setenv("PGPASSWORD", "pass")
	config, err = parseConnectionString("dbname=test")
	assert.NoError(t, err)
	assert.Equal(t, "test", config.Database)
	assert.Equal(t, "pass", config.Password)
}

func TestKompirki(t *testing.T) {
	connConfig, _ := pgx.ParseConnectionString("user=postgres password=postgres sslmode=disable database=postgres")
	conn, _ := pgx.Connect(connConfig)
	data := [][]interface{}{{1, "a"}}
	source := pgx.CopyFromRows(data)
	i, err := conn.CopyFrom(pgx.Identifier{"kure"}, []string{"a", "c"}, source)
	fmt.Println(i)
	fmt.Println(err)
}
