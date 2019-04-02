package postgresql

import "log"

func (p *Postgresql) tableExists(tableName string) bool {
	stmt := "SELECT tablename FROM pg_tables WHERE tablename = $1 AND schemaname = $2;"
	result, err := p.db.Exec(stmt, tableName, p.Schema)
	if err != nil {
		log.Printf("E! Error checking for existence of metric table %s: %v", tableName, err)
		return false
	}
	if count, _ := result.RowsAffected(); count == 1 {
		p.Tables[tableName] = true
		return true
	}
	return false
}
