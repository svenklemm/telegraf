package postgresql

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/columns"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/db"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/utils"
	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/stretchr/testify/assert"
)

func TestPostgresqlMetricsFromMeasure(t *testing.T) {
	postgreSQL, metrics := prepareAllColumnsInOnePlaceNoJSON()
	err := postgreSQL.writeMetricsFromMeasure(metrics["m"][0].Name(), metrics["m"])
	assert.NoError(t, err)
	postgreSQL, metrics = prepareAllColumnsInOnePlaceTagsAndFieldsJSON()
	err = postgreSQL.writeMetricsFromMeasure(metrics["m"][0].Name(), metrics["m"])
	assert.NoError(t, err)
}

func TestPostgresqlIsAliveCalledOnWrite(t *testing.T) {
	postgreSQL, metrics := prepareAllColumnsInOnePlaceNoJSON()
	mockedDb := postgreSQL.db.(*mockDb)
	mockedDb.isAliveResponses = []bool{true}
	err := postgreSQL.Write(metrics["m"][:1])
	assert.NoError(t, err)
	assert.Equal(t, 1, mockedDb.currentIsAliveResponse)
}

func prepareAllColumnsInOnePlaceNoJSON() (*Postgresql, map[string][]telegraf.Metric) {
	oneMetric, _ := metric.New("m", map[string]string{"t": "tv"}, map[string]interface{}{"f": 1}, time.Now())
	twoMetric, _ := metric.New("m", map[string]string{"t2": "tv2"}, map[string]interface{}{"f2": 2}, time.Now())
	threeMetric, _ := metric.New("m", map[string]string{"t": "tv", "t2": "tv2"}, map[string]interface{}{"f": 3, "f2": 4}, time.Now())

	return &Postgresql{
		TagTableSuffix:  "_tag",
		DoSchemaUpdates: true,
		tables:          &mockTables{t: map[string]bool{"m": true}, missingCols: []int{}},
		rows:            &mockTransformer{rows: [][]interface{}{nil, nil, nil}},
		columns:         columns.NewMapper(false),
		db:              &mockDb{},
	}, map[string][]telegraf.Metric{
		"m": {oneMetric, twoMetric, threeMetric},
	}
}

func prepareAllColumnsInOnePlaceTagsAndFieldsJSON() (*Postgresql, map[string][]telegraf.Metric) {
	oneMetric, _ := metric.New("m", map[string]string{"t": "tv"}, map[string]interface{}{"f": 1}, time.Now())
	twoMetric, _ := metric.New("m", map[string]string{"t2": "tv2"}, map[string]interface{}{"f2": 2}, time.Now())
	threeMetric, _ := metric.New("m", map[string]string{"t": "tv", "t2": "tv2"}, map[string]interface{}{"f": 3, "f2": 4}, time.Now())

	return &Postgresql{
		TagTableSuffix:    "_tag",
		DoSchemaUpdates:   true,
		TagsAsForeignkeys: false,
		tables:            &mockTables{t: map[string]bool{"m": true}, missingCols: []int{}},
		columns:           columns.NewMapper(false),
		rows:              &mockTransformer{rows: [][]interface{}{nil, nil, nil}},
		db:                &mockDb{},
	}, map[string][]telegraf.Metric{
		"m": {oneMetric, twoMetric, threeMetric},
	}
}

type mockTables struct {
	t           map[string]bool
	createErr   error
	missingCols []int
	mismatchErr error
	addColsErr  error
}

func (m *mockTables) Exists(db db.Wrapper, tableName string) bool {
	return m.t[tableName]
}
func (m *mockTables) CreateTable(db db.Wrapper, tableName string, colDetails *utils.TargetColumns, tagTable bool) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.t[tableName] = true
	return nil
}
func (m *mockTables) FindColumnMismatch(db db.Wrapper, tableName string, colDetails *utils.TargetColumns) ([]int, error) {
	return m.missingCols, m.mismatchErr
}
func (m *mockTables) AddColumnsToTable(db db.Wrapper, tableName string, columnIndices []int, colDetails *utils.TargetColumns) error {
	return m.addColsErr
}

type mockTransformer struct {
	rows    [][]interface{}
	current int
	rowErr  error
}

func (mt *mockTransformer) createRowFromMetric(
	db db.Wrapper,
	numColumns int,
	metric telegraf.Metric,
	targetColumns,
	targetTagColumns *utils.TargetColumns) ([]interface{}, *utils.ErrorBundle) {
	if mt.rowErr != nil {
		return nil, &utils.ErrorBundle{utils.PgErrorUnknown,mt.rowErr}
	}
	row := mt.rows[mt.current]
	mt.current++
	return row, nil
}

type mockDb struct {
	doCopyErr               error
	isAliveResponses        []bool
	currentIsAliveResponse  int
	secondsToSleepInIsAlive int64
}

func (m *mockDb) Exec(query string, args ...interface{}) (pgx.CommandTag, error) {
	return "", nil
}

func (m *mockDb) DoCopy(fullTableName *pgx.Identifier, colNames []string, batch [][]interface{}) *utils.ErrorBundle {
	return &utils.ErrorBundle{utils.PgErrorUnknown, m.doCopyErr}
}
func (m *mockDb) Query(query string, args ...interface{}) (*pgx.Rows, error) {
	return nil, nil
}
func (m *mockDb) QueryRow(query string, args ...interface{}) *pgx.Row {
	return nil
}
func (m *mockDb) Close() error {
	return nil
}

func (m *mockDb) IsAlive() bool {
	if m.secondsToSleepInIsAlive > 0 {
		time.Sleep(time.Duration(m.secondsToSleepInIsAlive) * time.Second)
	}
	if m.isAliveResponses == nil {
		return true
	}
	if m.currentIsAliveResponse >= len(m.isAliveResponses) {
		return m.isAliveResponses[len(m.isAliveResponses)]
	}
	which := m.currentIsAliveResponse
	m.currentIsAliveResponse++
	return m.isAliveResponses[which]
}
