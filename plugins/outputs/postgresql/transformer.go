package postgresql

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/db"
	"github.com/influxdata/telegraf/plugins/outputs/postgresql/utils"
)

type transformer interface {
	createRowFromMetric(
		db db.Wrapper,
		numColumns int,
		metric telegraf.Metric,
		targetColumns, targetTagColumns *utils.TargetColumns) ([]interface{}, *utils.ErrorBundle)
}

type defTransformer struct {
	tagsAsFK  bool
	tagsCache tagsCache
}

func newRowTransformer(tagsAsFK bool, tagsCache tagsCache) transformer {
	return &defTransformer{
		tagsAsFK:  tagsAsFK,
		tagsCache: tagsCache,
	}
}

func (dt *defTransformer) createRowFromMetric(
	db db.Wrapper,
	numColumns int,
	metric telegraf.Metric,
	targetColumns,
	targetTagColumns *utils.TargetColumns,
) ([]interface{}, *utils.ErrorBundle) {
	row := make([]interface{}, numColumns)
	// handle time
	row[0] = metric.Time()
	// handle tags and tag id
	if dt.tagsAsFK {
		tagID, err := dt.tagsCache.getTagID(db, targetTagColumns, metric)
		if err != nil {
			return nil, utils.DecodePgError(err)
		}
		row[1] = tagID
	} else {

		for _, tag := range metric.TagList() {
			targetIndex := targetColumns.Target[tag.Key]
			row[targetIndex] = tag.Value
		}
	}

	// handle fields
	for _, field := range metric.FieldList() {
		targetIndex := targetColumns.Target[field.Key]
		row[targetIndex] = field.Value
	}

	return row, nil
}
