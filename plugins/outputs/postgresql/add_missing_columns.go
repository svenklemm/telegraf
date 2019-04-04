package postgresql

import (
	"fmt"
	"log"
	"strings"
)

func (p *Postgresql) addMissingColumns(tableName string, columns []string, values []interface{}) (bool, error) {
	var quotedColumns = make([]string, len(columns))
	for i, column := range columns {
		quotedColumns[i] = quoteLiteral(column)
	}
	missingColumnsQuery := fmt.Sprintf(missingColumnsTemplate, strings.Join(quotedColumns, ","))
	result, err := p.db.Query(missingColumnsQuery, p.Schema, tableName)
	if err != nil {
		return false, err
	}
	defer result.Close()

	// some columns are missing
	retry := false
	var columnName string
	var isMissing bool
	currentColumn := 0
	for result.Next() {
		err := result.Scan(&columnName, &isMissing)
		if err != nil {
			log.Println(err)
			return false, nil
		}

		if !isMissing {
			currentColumn++
			continue
		}

		fullTableName := p.fullTableName(tableName)
		dataType := deriveDatatype(values[currentColumn])
		addColumnQuery := fmt.Sprintf(addColumnTemplate, fullTableName, quoteIdent(columnName), dataType)
		_, err = p.db.Exec(addColumnQuery)
		if err != nil {
			return false, err
		}
		retry = true
	}

	return retry, nil
}
