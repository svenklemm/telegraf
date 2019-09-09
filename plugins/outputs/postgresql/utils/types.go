package utils

import (
	"fmt"
	"github.com/jackc/pgx"
)

// ColumnRole specifies the role of a column in a metric.
// It helps map the columns to the DB.
type ColumnRole int

// ColumnRole available values.
const (
	TimeColType ColumnRole = iota + 1
	TagsIDColType
	TagColType
	FieldColType
)

func (c ColumnRole) String() string {
	switch c {
	case TimeColType:
		return "TimeColType"
	case TagsIDColType:
		return "TagsIDColType"
	case TagColType:
		return "TagColType"
	case FieldColType:
		return "FieldColType"
	default:
		return "programmer forgot to add col type to String() switch LoL"
	}
}

// PgDataType defines a string that represents a PostgreSQL data type.
type PgDataType string

// TargetColumns contains all the information needed to map a collection of
// metrics who belong to the same Measurement.
type TargetColumns struct {
	// the names the columns will have in the database
	Names []string
	// column name -> order number. where to place each column in rows
	// batched to the db
	Target map[string]int
	// the data type of each column should have in the db. used when checking
	// if the schema matches or it needs updates
	DataTypes []PgDataType
	// the role each column has, helps properly map the metric to the db
	Roles []ColumnRole
}

func (t *TargetColumns) String() string {
	return fmt.Sprintf("{Names:%v, Target: %v, DataTypes: %v, Roles: %v", t.Names, t.Target, t.DataTypes, t.Roles)
}

// DecodedPgError tells us what happened when executing a PG query
type DecodedPgError int

// ColumnRole available values.
const (
	PgErrorMissingColumn DecodedPgError = iota + 1
	PgErrorMissingTable
	PgErrorUnknown
)

// ErrorBundle combines the pg code and the actual error.
type ErrorBundle struct {
	Code DecodedPgError
	Err  error
}

// DecodePgError attempts to discover if an error was a PgError
// and decode it.
func DecodePgError(err error) *ErrorBundle {
	pgErr, ok := err.(pgx.PgError)
	if !ok {
		return &ErrorBundle{PgErrorUnknown, err}
	}
	var code DecodedPgError
	switch pgErr.Code {
	case "42703":
		code = PgErrorMissingColumn
	case "42P01":
		code = PgErrorMissingTable
	default:
		code = PgErrorUnknown
	}

	return &ErrorBundle{code, err}
}
