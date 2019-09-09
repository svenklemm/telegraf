package postgresql

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/columns"
	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/stretchr/testify/assert"
)

const (
	connStrAdmin    = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	nonAdminUser    = "grunt"
	connStrNonAdmin = "postgres://" + nonAdminUser + ":" + nonAdminUser + "@localhost:5432/postgres?sslmode=disable"
	testMetricName  = "metric name"
	testTagName     = "tag name"
	testTagVal      = "tag1"
	testFieldName   = "value name"
	testFieldVal    = int(1)
)

func prepareAndConnect(t *testing.T, foreignTags bool) (telegraf.Metric, *sql.DB, *Postgresql) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testMetric := testMetric(testMetricName, testTagVal, testFieldVal)

	postgres := &Postgresql{
		Connection:        connStrAdmin,
		Schema:            "public",
		TagsAsForeignkeys: foreignTags,
		DoSchemaUpdates:   true,
		TableTemplate:     "CREATE TABLE IF NOT EXISTS {TABLE}({COLUMNS})",
		TagTableSuffix:    "_tags",
	}

	// drop metric tables if exists

	db, err := sql.Open("pgx", connStrAdmin)
	assert.NoError(t, err, "Could not connect to test db")

	_, err = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, testMetric.Name()))
	assert.NoError(t, err, "Could not prepare db")
	_, err = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s%s"`, testMetric.Name(), postgres.TagTableSuffix))
	assert.NoError(t, err, "Could not prepare db")

	err = postgres.Connect()
	assert.NoError(t, err, "Could not connect")
	return testMetric, db, postgres
}

// testMetric Returns a simple test point:
//     measurement -> name
//     tags -> "tag":tag
//     value -> "value": value
//     time -> time.Now().UTC()
func testMetric(name string, tag string, value interface{}) telegraf.Metric {
	if value == nil {
		panic("Cannot use a nil value")
	}
	tags := map[string]string{testTagName: tag}
	pt, _ := metric.New(
		name,
		tags,
		map[string]interface{}{testFieldName: value},
		time.Now().UTC(),
	)
	return pt
}

func TestWriteToPostgres(t *testing.T) {
	testMetric, dbConn, postgres := prepareAndConnect(t, false)
	writeAndAssertSingleMetricNoJSON(t, testMetric, dbConn, postgres)
}

func TestWriteToPostgresMultipleRowsOneTag(t *testing.T) {
	tagsAsForeignKey := true
	testMetric, dbConn, postgres := prepareAndConnect(t, tagsAsForeignKey)
	defer dbConn.Close()

	// insert first metric
	err := postgres.Write([]telegraf.Metric{testMetric, testMetric})
	assert.NoError(t, err, "Could not write")

	// should have two rows
	row := dbConn.QueryRow(fmt.Sprintf(`SELECT count(*) FROM "%s"`, testMetric.Name()))
	var count int64
	err = row.Scan(&count)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, int64(2), count)

	sentTag, _ := testMetric.GetTag(testTagName)
	row = dbConn.QueryRow(fmt.Sprintf(`SELECT "%s", "%s" FROM "%s%s"`, columns.TagIDColumnName, testTagName, testMetric.Name(), postgres.TagTableSuffix))
	var tagID int64
	var tags string
	err = row.Scan(&tagID, &tags)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, int64(1), tagID)
	assert.Equal(t, sentTag, tags)
}

func TestWriteToPostgresAddNewTag(t *testing.T) {
	tagsAsForeignKey := true
	testMetricWithOneTag, dbConn, postgres := prepareAndConnect(t, tagsAsForeignKey)
	defer dbConn.Close()

	testMetricWithOneMoreTag := testMetric(testMetricName, "tag1", int(2))
	testMetricWithOneMoreTag.AddTag("second_tag", "tag2")
	// insert first two metric
	err := postgres.Write([]telegraf.Metric{testMetricWithOneTag, testMetricWithOneMoreTag})
	assert.NoError(t, err, "Could not write")

	// should have two rows
	row := dbConn.QueryRow(fmt.Sprintf(`SELECT count(*) FROM "%s"`, testMetricWithOneTag.Name()))
	var count int64
	err = row.Scan(&count)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, int64(2), count)

	// and two tagsets
	sentTag, _ := testMetricWithOneTag.GetTag(testTagName)
	row = dbConn.QueryRow(fmt.Sprintf(`SELECT "%s" FROM "%s%s" WHERE "%s"=1`, testTagName, testMetricWithOneTag.Name(), postgres.TagTableSuffix, columns.TagIDColumnName))
	var tagVal string
	err = row.Scan(&tagVal)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, sentTag, tagVal)

	row = dbConn.QueryRow(fmt.Sprintf(`SELECT "%s","%s" FROM "%s%s" WHERE "%s"=2`, testTagName, "second_tag", testMetricWithOneMoreTag.Name(), postgres.TagTableSuffix, columns.TagIDColumnName))
	var tagVal2 string
	err = row.Scan(&tagVal, &tagVal2)
	sentTag, _ = testMetricWithOneMoreTag.GetTag(testTagName)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, sentTag, tagVal)
	assert.Equal(t, "tag2", tagVal2)

	// insert new point with a third tagset
	testMetricWithThirdTag := testMetric(testMetricName, "tag1", int(2))
	testMetricWithThirdTag.AddTag("third_tag", "tag3")
	err = postgres.Write([]telegraf.Metric{testMetricWithThirdTag})
	assert.NoError(t, err, "Could not write")
	row = dbConn.QueryRow(fmt.Sprintf(`SELECT %s FROM "%s%s" WHERE "%s"=3`, "third_tag", testMetricWithThirdTag.Name(), postgres.TagTableSuffix, columns.TagIDColumnName))
	var tagVal3 string
	err = row.Scan(&tagVal3)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, "tag3", tagVal3)
}

func TestWriteToPostgresAddNewField(t *testing.T) {
	testMetric, dbConn, postgres := prepareAndConnect(t, false)
	defer dbConn.Close()

	// insert first metric
	writeAndAssertSingleMetricNoJSON(t, testMetric, dbConn, postgres)

	//insert second metric with one more field
	testMetric.AddField("field2", 1.0)
	testMetric.SetTime(time.Now())
	err := postgres.Write([]telegraf.Metric{testMetric})
	assert.NoError(t, err, "Could not write")

	rows, err := dbConn.Query(fmt.Sprintf(`SELECT "%s", "%s", "%s", field2 FROM "%s" ORDER BY "%s" ASC`, columns.TimeColumnName, testTagName, testFieldName, testMetric.Name(), columns.TimeColumnName))
	assert.NoError(t, err, "Could not check written results")
	var ts time.Time
	var tag string
	var value sql.NullInt64
	var field2 sql.NullFloat64
	rowNum := 1
	for rows.Next() {
		rows.Scan(&ts, &tag, &value, &field2)
		if rowNum == 1 {
			assert.False(t, field2.Valid)
		} else if rowNum == 2 {
			assert.Equal(t, 1.0, field2.Float64)
		} else {
			assert.FailNow(t, "more rows than expected")
		}
		rowNum++
	}

}

func writeAndAssertSingleMetricNoJSON(t *testing.T, testMetric telegraf.Metric, dbConn *sql.DB, postgres *Postgresql) {
	err := postgres.Write([]telegraf.Metric{testMetric})
	assert.NoError(t, err, "Could not write")

	// should have created table, all columns in the same table
	row := dbConn.QueryRow(fmt.Sprintf(`SELECT "%s", "%s", "%s" FROM "%s"`, columns.TimeColumnName, testTagName, testFieldName, testMetric.Name()))
	var ts time.Time
	var tag string
	var value int64
	err = row.Scan(&ts, &tag, &value)
	assert.NoError(t, err, "Could not check test results")

	sentTag, _ := testMetric.GetTag(testTagName)
	sentValue, _ := testMetric.GetField(testFieldName)
	sentTs := testMetric.Time()
	// postgres doesn't support nano seconds in timestamp
	sentTsNanoSecondOffset := sentTs.Nanosecond()
	nanoSeconds := sentTsNanoSecondOffset % 1000
	sentTs = sentTs.Add(time.Duration(-nanoSeconds) * time.Nanosecond)
	if !ts.UTC().Equal(sentTs) || tag != sentTag || value != sentValue.(int64) {
		assert.Fail(t, fmt.Sprintf("Expected: %v, %v, %v; Received: %v, %v, %v",
			sentTs, sentTag, sentValue,
			ts.UTC(), tag, value))
	}
}

func TestWriteToPostgresMultipleMetrics(t *testing.T) {
	tagsAsForeignKey := true
	testMetric, dbConn, postgres := prepareAndConnect(t, tagsAsForeignKey)
	defer dbConn.Close()
	dbConn.Exec(`DROP TABLE IF EXISTS "` + testMetric.Name() + `2"`)
	dbConn.Exec(`DROP TABLE IF EXISTS "` + testMetric.Name() + `2_tag"`)
	testMetricInSecondMeasurement, _ := metric.New(testMetric.Name()+"2", testMetric.Tags(), testMetric.Fields(), testMetric.Time().Add(time.Second))
	// insert first metric
	err := postgres.Write([]telegraf.Metric{testMetric, testMetric, testMetricInSecondMeasurement})
	assert.NoError(t, err, "Could not write")

	// should have created table, all columns in the same table
	rows, _ := dbConn.Query(fmt.Sprintf(`SELECT "%s", "%s", "%s" FROM "%s"`, columns.TimeColumnName, columns.TagIDColumnName, testFieldName, testMetric.Name()))
	// check results for testMetric if in db
	for i := 0; i < 2; i++ {
		var ts time.Time
		var tagID int64
		var value int64
		rows.Next()
		err = rows.Scan(&ts, &tagID, &value)
		assert.NoError(t, err, "Could not check test results")

		sentValue, _ := testMetric.GetField(testFieldName)
		sentTs := testMetric.Time()
		// postgres doesn't support nano seconds in timestamp
		sentTsNanoSecondOffset := sentTs.Nanosecond()
		nanoSeconds := sentTsNanoSecondOffset % 1000
		sentTs = sentTs.Add(time.Duration(-nanoSeconds) * time.Nanosecond)
		if !ts.UTC().Equal(sentTs.UTC()) {
			assert.Fail(t, fmt.Sprintf("Expected: %v; Received: %v", sentTs, ts.UTC()))
		}

		assert.Equal(t, int64(1), tagID)
		assert.Equal(t, sentValue.(int64), value)

		sentTag, _ := testMetric.GetTag(testTagName)
		row := dbConn.QueryRow(fmt.Sprintf(`SELECT "%s", "%s" FROM "%s%s"`, columns.TagIDColumnName, testTagName, testMetric.Name(), postgres.TagTableSuffix))
		tagID = 0
		var tags string
		err = row.Scan(&tagID, &tags)
		assert.NoError(t, err, "Could not check test results")
		assert.Equal(t, int64(1), tagID)
		assert.Equal(t, sentTag, tags)
	}
	// check results for second metric
	row := dbConn.QueryRow(fmt.Sprintf(`SELECT "%s", "%s", "%s" FROM "%s"`, columns.TimeColumnName, columns.TagIDColumnName, testFieldName, testMetricInSecondMeasurement.Name()))
	var ts time.Time
	var tagID int64
	var value int64
	err = row.Scan(&ts, &tagID, &value)
	assert.NoError(t, err, "Could not check test results")

	sentValue, _ := testMetricInSecondMeasurement.GetField(testFieldName)
	sentTs := testMetricInSecondMeasurement.Time()
	// postgres doesn't support nano seconds in timestamp
	sentTsNanoSecondOffset := sentTs.Nanosecond()
	nanoSeconds := sentTsNanoSecondOffset % 1000
	sentTs = sentTs.Add(time.Duration(-nanoSeconds) * time.Nanosecond)
	if !ts.UTC().Equal(sentTs.UTC()) {
		assert.Fail(t, fmt.Sprintf("Expected: %v; Received: %v", sentTs, ts.UTC()))
	}

	assert.Equal(t, int64(1), tagID)
	assert.Equal(t, sentValue.(int64), value)
}

func TestPerformanceIsAcceptable(t *testing.T) {
	_, db, postgres := prepareAndConnect(t, false)
	defer db.Close()
	numMetricsPerMeasure := 100000
	numTags := 5
	numDiffValuesForEachTag := 5
	numFields := 10
	numMeasures := 1
	metrics := make([]telegraf.Metric, numMeasures*numMetricsPerMeasure)
	for measureInd := 0; measureInd < numMeasures; measureInd++ {
		for numMetric := 0; numMetric < numMetricsPerMeasure; numMetric++ {
			tags := map[string]string{}
			for tag := 0; tag < numTags; tag++ {
				randNum := rand.Intn(numDiffValuesForEachTag)
				tags[fmt.Sprintf("tag_%d", tag)] = strconv.Itoa(randNum)
			}
			fields := map[string]interface{}{}
			for field := 0; field < numFields; field++ {
				fields[fmt.Sprintf("field_%d", field)] = rand.Float64()
			}
			metricName := "m_" + strconv.Itoa(measureInd)
			m, _ := metric.New(metricName, tags, fields, time.Now())
			metrics[measureInd*numMetricsPerMeasure+numMetric] = m
		}
	}

	start := time.Now()
	err := postgres.Write(metrics)
	assert.NoError(t, err)
	end := time.Since(start)
	t.Log("Wrote " + strconv.Itoa(numMeasures*numMetricsPerMeasure) + " metrics in " + end.String())
}

func TestPostgresBatching(t *testing.T) {
	_, db, postgres := prepareAndConnect(t, false)
	defer db.Close()
	numMetricsPerMeasure := 5
	numMeasures := 2
	metrics := make([]telegraf.Metric, numMeasures*numMetricsPerMeasure)
	for measureInd := 0; measureInd < numMeasures; measureInd++ {
		metricName := "m_" + strconv.Itoa(measureInd)
		db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS ` + metricName))
		for numMetric := 0; numMetric < numMetricsPerMeasure; numMetric++ {
			tags := map[string]string{}
			fields := map[string]interface{}{"f": 1}
			m, _ := metric.New(metricName, tags, fields, time.Now())
			metrics[measureInd*numMetricsPerMeasure+numMetric] = m
		}
	}

	err := postgres.Write(metrics)
	assert.NoError(t, err)
	err = postgres.Write(metrics)
	assert.NoError(t, err)
	// check num rows inserted by transaction id should be 'numMetricsPerMeasure' for
	// both transactions, for all measures
	for measureInd := 0; measureInd < numMeasures; measureInd++ {
		metricName := "m_" + strconv.Itoa(measureInd)
		rows, err := db.Query(`select count(*) from ` + metricName + ` group by xmin`)
		assert.NoError(t, err)
		var count int64
		rows.Next()
		require.NoError(t, rows.Scan(&count))
		assert.Equal(t, int64(numMetricsPerMeasure), count)
		require.NoError(t,rows.Close())
	}
}

func TestNotOwnerOfExistingTable(t *testing.T) {
	TestWriteToPostgres(t)
	conf, _ := pgx.ParseConnectionString(connStrAdmin)
	db, _ := pgx.Connect(conf)
	db.Exec("CREATE USER " + nonAdminUser + " WITH LOGIN PASSWORD '" + nonAdminUser + "';")
	db.Close()
	postgres := &Postgresql{
		Connection:        connStrNonAdmin,
		Schema:            "public",
		TagsAsForeignkeys: false,
		DoSchemaUpdates:   true,
		TableTemplate:     "CREATE TABLE IF NOT EXISTS {TABLE}({COLUMNS})",
		TagTableSuffix:    "_tags",
	}
	err := postgres.Connect()
	defer postgres.Close()
	assert.NoError(t, err)
	err = postgres.Write([]telegraf.Metric{testMetric(testMetricName, "tag2", 2)})
	assert.Error(t, err)

}
