package columns

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/utils"
)

// Mapper knows how to generate the column details for the main and tags table in the db
type Mapper interface {
	// Target iterates through an array of 'metrics' visiting only those indexed by 'indices'
	// and depending on 'tagsAsFK', 'tagsAsJSON', and 'fieldsAsJSON' generate the
	// desired columns (their name, type and which role they play) for both the
	// main metrics table in the DB, and if tagsAsFK == true for the tags table.
	Target(metrics []telegraf.Metric) (*utils.TargetColumns, *utils.TargetColumns)
}

type defMapper struct {
	initTargetColumns targetColumnInitializer
	tagsAsFK          bool
	tagsAsJSON        bool
	fieldsAsJSON      bool
}

// NewMapper returns a new implementation of the columns.Mapper interface.
func NewMapper(tagsAsFK bool) Mapper {
	initializer := getInitialColumnsGenerator(tagsAsFK)
	return &defMapper{
		tagsAsFK:          tagsAsFK,
		initTargetColumns: initializer,
	}
}

// Target iterates through an array of 'metrics' visiting only those indexed by 'indices'
// and depending on 'tagsAsFK', 'tagsAsJSON', and 'fieldsAsJSON' generate the
// desired columns (their name, type and which role they play) for both the
// main metrics table in the DB, and if tagsAsFK == true for the tags table.
func (d *defMapper) Target(metrics []telegraf.Metric) (*utils.TargetColumns, *utils.TargetColumns) {
	columns, tagColumns := d.initTargetColumns()
	if d.tagsAsJSON && d.fieldsAsJSON {
		// if json is used for both, that's all the columns you need
		return columns, tagColumns
	}

	alreadyMapped := map[string]bool{}
	// Iterate the metrics and populate all the resulting required columns
	// e.g. metric1(tags:[t1], fields:[f1,f2]), metric2(tags:[t2],fields:[f2, f3])
	// => columns = [time, t1, f1, f2, t2, f3], tagColumns = nil
	// if tagsAsFK == true
	//    columns = [time, tagID, f1, f2, f3], tagColumns = [tagID, t1, t2]
	// if tagsAsFK == true && fieldsAsJSON = true
	//    cols = [time, tagID, fields], tagCols = [tagID, t1, t2]
	for _, metric := range metrics {
		if !d.tagsAsJSON {
			whichColumns := columns
			if d.tagsAsFK {
				whichColumns = tagColumns
			}
			mapTags(metric.TagList(), alreadyMapped, whichColumns)
		}

		mapFields(metric.FieldList(), alreadyMapped, columns)
	}

	return columns, tagColumns
}
