package columns

import "github.com/influxdata/telegraf/plugins/outputs/postgresql/utils"

// a function type that generates column details for the main, and tags table in the db
type targetColumnInitializer func() (*utils.TargetColumns, *utils.TargetColumns)

// constants used for populating the 'targetColumnInit' map (for better readability)
const (
	cTagsAsFK = true
)

// Since some of the target columns for the tables in the database don't
// depend on the metrics received, but on the plugin config, we can have
// constant initializer functions. It is always known that the 'time'
// column goes first in the main table, then if the tags are kept in a
// separate table you need to add the 'tag_id' column...
// This map contains an initializer both cases of tagsAsFK.
func getInitialColumnsGenerator(tagsAsFK bool) targetColumnInitializer {
	return standardColumns[tagsAsFK]
}

var standardColumns = map[bool]targetColumnInitializer{
	cTagsAsFK:  tagsAsFKInit,
	!cTagsAsFK: vanillaColumns,
}

func tagsAsFKInit() (*utils.TargetColumns, *utils.TargetColumns) {
	return &utils.TargetColumns{
		Names:     []string{TimeColumnName, TagIDColumnName},
		DataTypes: []utils.PgDataType{TimeColumnDataType, TagIDColumnDataType},
		Target:    map[string]int{TimeColumnName: 0, TagIDColumnName: 1},
		Roles:     []utils.ColumnRole{utils.TimeColType, utils.TagsIDColType},
	}, &utils.TargetColumns{
		Names:     []string{TagIDColumnName},
		DataTypes: []utils.PgDataType{TagIDColumnDataTypeAsPK},
		Target:    map[string]int{TagIDColumnName: 0},
		Roles:     []utils.ColumnRole{utils.TagsIDColType},
	}
}

func vanillaColumns() (*utils.TargetColumns, *utils.TargetColumns) {
	return &utils.TargetColumns{
		Names:     []string{TimeColumnName},
		DataTypes: []utils.PgDataType{TimeColumnDataType},
		Target:    map[string]int{TimeColumnName: 0},
		Roles:     []utils.ColumnRole{utils.TimeColType},
	}, nil
}
