package fakeinstalls

import (
	"fmt"
	"math/rand"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type InstallMetrics struct {
	NumFakes int
}

type fakehost struct {
	clusterID int
	cpuType   string
}

func (_ *InstallMetrics) Description() string {
	return "Make up fake metrics about fake installs!"
}

var sampleConfig = `
  ## Number of installs to generate fake metrics for
  numfakes = 1000
`

func (_ *InstallMetrics) SampleConfig() string {
	return sampleConfig
}

func (s *InstallMetrics) Gather(acc telegraf.Accumulator) error {
	fields := make(map[string]interface{})
	tags := make(map[string]string)
	fakeHosts := make([]fakehost, 0)
	baseHosts := 1
	if s.NumFakes/75 > 1 {
		baseHosts = s.NumFakes / 75
	}
	for i := 0; i <= baseHosts; i++ {
		fakeHosts = append(fakeHosts, fakehost{clusterID: i, cpuType: "mooLake"})
	}
	for i := 0; i < s.NumFakes; i++ {
		fields = make(map[string]interface{})
		tags = make(map[string]string)
		tags["installName"] = fmt.Sprintf("install-%d", i)
		tags["hostName"] = fmt.Sprintf("fakepod-%d", fakeHosts[i/75].clusterID)
		tags["clusterID"] = fmt.Sprintf("%d", fakeHosts[i/75].clusterID)
		tags["cpuType"] = fakeHosts[i/75].cpuType
		tags["plan"] = fmt.Sprintf("p-%d", i%9)
		tags["accountID"] = fmt.Sprintf("account-%d", i%66)
		fields["diskUsed"] = 100 * (rand.Intn(10) + 1)
		// fields["diskUsed"] = 500
		delete(tags, "host")

		acc.AddFields("fakeinstalls", fields, tags)
	}
	return nil
}

func init() {
	inputs.Add("fakeinstalls", func() telegraf.Input {
		return &InstallMetrics{}
	})
}
