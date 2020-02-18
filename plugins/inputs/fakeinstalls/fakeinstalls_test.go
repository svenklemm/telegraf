package fakeinstalls

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
)

func createTestMetric() telegraf.Metric {
	metric, _ := metric.New("m1",
		map[string]string{"metric_tag": "from_metric"},
		map[string]interface{}{"value": int64(1)},
		time.Now(),
	)
	return metric
}

func TestGeneratesRightNumberOfMetrics(t *testing.T) {

	f := InstallMetrics{
		NumFakes: 100,
	}

	var acc testutil.Accumulator
	err := f.Gather(&acc)
	if err != nil {
		t.Fatal(err)
	}

	// // tags1 := map[string]string{
	// // 	"clusterID": "1",
	// // 	"cpuType":   "mooLake",
	// // }

	// fields1 := map[string]interface{}{
	// 	"diskUsed": 500,
	// }
	// for _, p := range acc.Metrics {
	// 	fmt.Printf("%v\n", p.Fields)
	// }
	// fmt.Println(len(acc.Metrics))
	assert.Equal(t, len(acc.Metrics), 100)
	// acc.AssertContainsTaggedFields(t, "fakeinstalls", fields1, tags1)
	// acc.AssertContainsFields(t, "fakeinstalls", fields1)

	// processor := InstallMetrics{NumFakes: 100}
	// processed := processor.Gather(createTestMetric())
	// tags := processed[0].Tags()

	// value, present := tags["metric_tag"]
	// assert.True(t, present, "Tag of metric was not present")
	// assert.Equal(t, "from_metric", value, "Value of Tag was changed")
}

func TestMetricsHaveRightTagsAndFields(t *testing.T) {

	f := InstallMetrics{
		NumFakes: 1,
	}

	var acc testutil.Accumulator
	err := f.Gather(&acc)
	if err != nil {
		t.Fatal(err)
	}

	metrics := acc.Metrics[0]
	assert.Contains(t, metrics.Tags, "installName")
	assert.Contains(t, metrics.Tags, "hostName")
	assert.Contains(t, metrics.Tags, "clusterID")
	assert.Contains(t, metrics.Tags, "cpuType")
	assert.Contains(t, metrics.Tags, "plan")
	assert.Contains(t, metrics.Tags, "accountID")
	assert.Contains(t, metrics.Fields, "diskUsed")

}
