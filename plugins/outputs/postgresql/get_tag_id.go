package postgresql

import (
	"fmt"
	"log"
	"strings"

	"github.com/influxdata/telegraf"
)

const (
	selectTagIDTemplate    = "SELECT tag_id FROM %s WHERE %s"
	missingColumnsTemplate = "SELECT c FROM unnest(array[%s]) AS c WHERE NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE column_name=c AND table_schema=$1 AND table_name=$2)"
	addColumnTemplate      = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;"
	tagDataType            = "TEXT"
)

func (p *Postgresql) getTagID(metric telegraf.Metric) (int, error) {
	var tagID int
	var whereColumns []string
	var whereValues []interface{}
	tablename := metric.Name()

	if p.TagsAsJsonb && len(metric.Tags()) > 0 {
		d, err := buildJsonbTags(metric.Tags())
		if err != nil {
			return tagID, err
		}

		whereColumns = append(whereColumns, "tags")
		whereValues = append(whereValues, d)
	} else {
		for column, value := range metric.Tags() {
			whereColumns = append(whereColumns, column)
			whereValues = append(whereValues, value)
		}
	}

	whereParts := make([]string, len(whereColumns))
	for i, column := range whereColumns {
		whereParts[i] = fmt.Sprintf("%s = $%d", quoteIdent(column), i+1)
	}

	tagsTableName := tablename + p.TagTableSuffix
	tagsTableFullName := p.fullTableName(tagsTableName)
	query := fmt.Sprintf(selectTagIDTemplate, tagsTableFullName, strings.Join(whereParts, " AND "))

	err := p.db.QueryRow(query, whereValues...).Scan(&tagID)
	if err == nil {
		return tagID, nil
	}
	query = p.generateInsert(tagsTableName, whereColumns) + " RETURNING tag_id"
	err = p.db.QueryRow(query, whereValues...).Scan(&tagID)
	if err == nil {
		return tagID, nil
	}

	// check if insert error was caused by column mismatch

	// if tags are jsonb, there shouldn't be a column mismatch
	if p.TagsAsJsonb {
		return tagID, err
	}

	// check for missing columns
	log.Printf("E! Error during insert: %v", err)
	var quotedColumns = make([]string, len(whereColumns))
	for i, column := range whereColumns {
		quotedColumns[i] = quoteLiteral(column)
	}
	missingColumnsQuery := fmt.Sprintf(missingColumnsTemplate, strings.Join(quotedColumns, ","))
	result, err := p.db.Query(missingColumnsQuery, p.Schema, tagsTableName)
	if err != nil {
		return tagID, err
	}
	defer result.Close()

	// some columns are missing
	retry := false
	var missingColumn string
	for result.Next() {
		err := result.Scan(&missingColumn)
		if err != nil {
			log.Println(err)
		}

		addColumnQuery := fmt.Sprintf(addColumnTemplate, tagsTableFullName, quoteIdent(missingColumn), tagDataType)
		_, err = p.db.Exec(addColumnQuery)
		if err != nil {
			return tagID, err
		}
		retry = true
	}

	// We added some columns and insert might work now. Try again immediately to
	// avoid long lead time in getting metrics when there are several columns missing
	// from the original create statement and they get added in small drops.
	if retry {
		err := p.db.QueryRow(query, whereValues...).Scan(&tagID)
		if err != nil {
			return tagID, err
		}
	}
	return tagID, nil
}
