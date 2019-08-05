package utils

import "fmt"

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
