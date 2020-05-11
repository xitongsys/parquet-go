package marshal

import (
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

//Marshal function for CSV like data
func MarshalCSV(records []interface{}, bgn int, end int, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	res := make(map[string]*layout.Table)
	if ln := len(records); ln <= 0 {
		return &res, nil
	}

	for i := 0; i < len(records[0].([]interface{})); i++ {
		pathStr := schemaHandler.GetRootInName() + "." + schemaHandler.Infos[i+1].InName
		table := layout.NewEmptyTable()
		res[pathStr] = table
		table.Path = common.StrToPath(pathStr)
		table.MaxDefinitionLevel = 1
		table.MaxRepetitionLevel = 0
		table.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
		table.Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
		table.Info = schemaHandler.Infos[i+1]
		// Pre-allocate these arrays for efficiency
		table.Values = make([]interface{}, 0, end-bgn)
		table.RepetitionLevels = make([]int32, 0, end-bgn)
		table.DefinitionLevels = make([]int32, 0, end-bgn)

		for j := bgn; j < end; j++ {
			rec := records[j].([]interface{})[i]
			table.Values = append(table.Values, rec)
			table.RepetitionLevels = append(table.RepetitionLevels, 0)

			if rec == nil {
				table.DefinitionLevels = append(table.DefinitionLevels, 0)
			} else {
				table.DefinitionLevels = append(table.DefinitionLevels, 1)
			}
		}
	}
	return &res, nil
}
