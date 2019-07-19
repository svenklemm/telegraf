package postgresql

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/stretchr/testify/assert"
)

func prepareAndConnect(t *testing.T, foreignTags, jsonTags, jsonFields bool) (telegraf.Metric, *sql.DB, *Postgresql) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testAddress := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

	testMetric := testMetric("metric name", "tag1", int(1))

	postgres := &Postgresql{
		Connection:        testAddress,
		Schema:            "public",
		TagsAsForeignkeys: foreignTags,
		TagsAsJsonb:       jsonTags,
		FieldsAsJsonb:     jsonFields,
		DoSchemaUpdates:   true,
		TableTemplate:     "CREATE TABLE IF NOT EXISTS {TABLE}({COLUMNS})",
		TagTableSuffix:    "_tags",
	}

	// drop metric tables if exists

	db, err := sql.Open("pgx", testAddress)
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
	tags := map[string]string{"tag": tag}
	pt, _ := metric.New(
		name,
		tags,
		map[string]interface{}{"value": value},
		time.Now().UTC(),
	)
	return pt
}

func TestWriteToPostgres(t *testing.T) {
	testMetric, dbConn, postgres := prepareAndConnect(t, false, false, false)
	writeAndAssertSingleMetricNoJSON(t, testMetric, dbConn, postgres)
}

func TestWriteToPostgresJsonTags(t *testing.T) {
	tagsAsForeignKey := false
	tagsAsJSON := true
	fieldsAsJSON := false
	testMetric, dbConn, postgres := prepareAndConnect(t, tagsAsForeignKey, tagsAsJSON, fieldsAsJSON)
	defer dbConn.Close()

	// insert first metric
	err := postgres.Write([]telegraf.Metric{testMetric})
	assert.NoError(t, err, "Could not write")

	// should have created table, all columns in the same table
	row := dbConn.QueryRow(fmt.Sprintf(`SELECT time, tags, value FROM "%s"`, testMetric.Name()))
	var ts time.Time
	var tags string
	var value int64
	err = row.Scan(&ts, &tags, &value)
	assert.NoError(t, err, "Could not check test results")

	sentTag, _ := testMetric.GetTag("tag")
	sentTagJSON := fmt.Sprintf(`{"tag": "%s"}`, sentTag)
	sentValue, _ := testMetric.GetField("value")
	sentTs := testMetric.Time()
	// postgres doesn't support nano seconds in timestamp
	sentTsNanoSecondOffset := sentTs.Nanosecond()
	nanoSeconds := sentTsNanoSecondOffset % 1000
	sentTs = sentTs.Add(time.Duration(-nanoSeconds) * time.Nanosecond)
	if !ts.UTC().Equal(sentTs) || tags != sentTagJSON || value != sentValue.(int64) {
		assert.Fail(t, fmt.Sprintf("Expected: %v, %v, %v; Received: %v, %v, %v",
			sentTs, sentTagJSON, sentValue,
			ts.UTC(), tags, value))
	}
}

func TestWriteToPostgresJsonTagsAsForeignTable(t *testing.T) {
	tagsAsForeignKey := true
	tagsAsJSON := true
	fieldsAsJSON := false
	testMetric, dbConn, postgres := prepareAndConnect(t, tagsAsForeignKey, tagsAsJSON, fieldsAsJSON)
	defer dbConn.Close()

	// insert first metric
	err := postgres.Write([]telegraf.Metric{testMetric})
	assert.NoError(t, err, "Could not write")

	// should have created table, all columns in the same table
	row := dbConn.QueryRow(fmt.Sprintf(`SELECT time, tag_id, value FROM "%s"`, testMetric.Name()))
	var ts time.Time
	var tagID int64
	var value int64
	err = row.Scan(&ts, &tagID, &value)
	assert.NoError(t, err, "Could not check test results")

	sentValue, _ := testMetric.GetField("value")
	sentTs := testMetric.Time()
	// postgres doesn't support nano seconds in timestamp
	sentTsNanoSecondOffset := sentTs.Nanosecond()
	nanoSeconds := sentTsNanoSecondOffset % 1000
	sentTs = sentTs.Add(time.Duration(-nanoSeconds) * time.Nanosecond)
	if !ts.UTC().Equal(sentTs) || tagID != 1 || value != sentValue.(int64) {
		assert.Fail(t, fmt.Sprintf("Expected: %v, %v, %v; Received: %v, %v, %v",
			sentTs, 1, sentValue,
			ts.UTC(), tagID, value))
	}

	sentTag, _ := testMetric.GetTag("tag")
	sentTagJSON := fmt.Sprintf(`{"tag": "%s"}`, sentTag)
	row = dbConn.QueryRow(fmt.Sprintf(`SELECT tag_id, tags FROM "%s%s"`, testMetric.Name(), postgres.TagTableSuffix))
	tagID = 0
	var tags string
	err = row.Scan(&tagID, &tags)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, int64(1), tagID)
	assert.Equal(t, sentTagJSON, tags)
}

func TestWriteToPostgresMultipleRowsOneTag(t *testing.T) {
	tagsAsForeignKey := true
	tagsAsJSON := true
	fieldsAsJSON := false
	testMetric, dbConn, postgres := prepareAndConnect(t, tagsAsForeignKey, tagsAsJSON, fieldsAsJSON)
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

	sentTag, _ := testMetric.GetTag("tag")
	sentTagJSON := fmt.Sprintf(`{"tag": "%s"}`, sentTag)
	row = dbConn.QueryRow(fmt.Sprintf(`SELECT tag_id, tags FROM "%s%s"`, testMetric.Name(), postgres.TagTableSuffix))
	var tagID int64
	var tags string
	err = row.Scan(&tagID, &tags)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, int64(1), tagID)
	assert.Equal(t, sentTagJSON, tags)
}

func TestWriteToPostgresAddNewTag(t *testing.T) {
	tagsAsForeignKey := true
	tagsAsJSON := true
	fieldsAsJSON := false
	testMetricWithOneTag, dbConn, postgres := prepareAndConnect(t, tagsAsForeignKey, tagsAsJSON, fieldsAsJSON)
	defer dbConn.Close()

	testMetricWithOneMoreTag := testMetric("metric name", "tag1", int(2))
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
	sentTag, _ := testMetricWithOneTag.GetTag("tag")
	sentTagJSON := fmt.Sprintf(`{"tag": "%s"}`, sentTag)
	row = dbConn.QueryRow(fmt.Sprintf(`SELECT tags FROM "%s%s" WHERE tag_id=1`, testMetricWithOneTag.Name(), postgres.TagTableSuffix))
	var tags string
	err = row.Scan(&tags)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, sentTagJSON, tags)

	secondSentTagsJSON := `{"tag": "tag1", "second_tag": "tag2"}`
	row = dbConn.QueryRow(fmt.Sprintf(`SELECT tags FROM "%s%s" WHERE tag_id=2`, testMetricWithOneMoreTag.Name(), postgres.TagTableSuffix))
	err = row.Scan(&tags)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, secondSentTagsJSON, tags)

	// insert new point with a third tagset
	testMetricWithThirdTag := testMetric("metric name", "tag1", int(2))
	testMetricWithThirdTag.AddTag("third_tag", "tag3")
	err = postgres.Write([]telegraf.Metric{testMetricWithThirdTag})
	assert.NoError(t, err, "Could not write")
	thirdSentTagsJSON := `{"tag": "tag1", "third_tag": "tag3"}`
	row = dbConn.QueryRow(fmt.Sprintf(`SELECT tags FROM "%s%s" WHERE tag_id=3`, testMetricWithThirdTag.Name(), postgres.TagTableSuffix))
	err = row.Scan(&tags)
	assert.NoError(t, err, "Could not check test results")
	assert.Equal(t, thirdSentTagsJSON, tags)
}

func TestWriteToPostgresAddNewField(t *testing.T) {
	testMetric, dbConn, postgres := prepareAndConnect(t, false, false, false)
	defer dbConn.Close()

	// insert first metric
	writeAndAssertSingleMetricNoJSON(t, testMetric, dbConn, postgres)

	//insert second metric with one more field
	testMetric.AddField("field2", 1.0)
	testMetric.SetTime(time.Now())
	err := postgres.Write([]telegraf.Metric{testMetric})
	assert.NoError(t, err, "Could not write")

	rows, err := dbConn.Query(fmt.Sprintf(`SELECT time, tag, value, field2 FROM "%s" ORDER BY time ASC`, testMetric.Name()))
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
	row := dbConn.QueryRow(fmt.Sprintf(`SELECT time, tag, value FROM "%s"`, testMetric.Name()))
	var ts time.Time
	var tag string
	var value int64
	err = row.Scan(&ts, &tag, &value)
	assert.NoError(t, err, "Could not check test results")

	sentTag, _ := testMetric.GetTag("tag")
	sentValue, _ := testMetric.GetField("value")
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
	tagsAsJSON := true
	fieldsAsJSON := false
	testMetric, dbConn, postgres := prepareAndConnect(t, tagsAsForeignKey, tagsAsJSON, fieldsAsJSON)
	defer dbConn.Close()
	dbConn.Exec(`DROP TABLE IF EXISTS "` + testMetric.Name() + `2"`)
	dbConn.Exec(`DROP TABLE IF EXISTS "` + testMetric.Name() + `2_tag"`)
	testMetricInSecondMeasurement, _ := metric.New(testMetric.Name()+"2", testMetric.Tags(), testMetric.Fields(), testMetric.Time().Add(time.Second))
	// insert first metric
	err := postgres.Write([]telegraf.Metric{testMetric, testMetric, testMetricInSecondMeasurement})
	assert.NoError(t, err, "Could not write")

	// should have created table, all columns in the same table
	rows, _ := dbConn.Query(fmt.Sprintf(`SELECT time, tag_id, value FROM "%s"`, testMetric.Name()))
	// check results for testMetric if in db
	for i := 0; i < 2; i++ {
		var ts time.Time
		var tagID int64
		var value int64
		rows.Next()
		err = rows.Scan(&ts, &tagID, &value)
		assert.NoError(t, err, "Could not check test results")

		sentValue, _ := testMetric.GetField("value")
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

		sentTag, _ := testMetric.GetTag("tag")
		sentTagJSON := fmt.Sprintf(`{"tag": "%s"}`, sentTag)
		row := dbConn.QueryRow(fmt.Sprintf(`SELECT tag_id, tags FROM "%s%s"`, testMetric.Name(), postgres.TagTableSuffix))
		tagID = 0
		var tags string
		err = row.Scan(&tagID, &tags)
		assert.NoError(t, err, "Could not check test results")
		assert.Equal(t, int64(1), tagID)
		assert.Equal(t, sentTagJSON, tags)
	}
	// check results for second metric
	row := dbConn.QueryRow(fmt.Sprintf(`SELECT time, tag_id, value FROM "%s"`, testMetricInSecondMeasurement.Name()))
	var ts time.Time
	var tagID int64
	var value int64
	err = row.Scan(&ts, &tagID, &value)
	assert.NoError(t, err, "Could not check test results")

	sentValue, _ := testMetricInSecondMeasurement.GetField("value")
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
	_, db, postgres := prepareAndConnect(t, false, false, false)
	defer db.Close()
	numMetricsPerMeasure := 10000
	numTags := 5
	numDiffValuesForEachTag := 5
	numFields := 10
	numMeasures := 2
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
	_, db, postgres := prepareAndConnect(t, false, false, false)
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
		rows.Scan(&count)
		assert.Equal(t, int64(numMetricsPerMeasure), count)
		rows.Close()
	}
}
