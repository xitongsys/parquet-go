package marshal

import (
	"github.com/syucream/parquet-go/common"
	"github.com/syucream/parquet-go/layout"
	"github.com/syucream/parquet-go/parquet"
	"github.com/syucream/parquet-go/schema"
)

//Marshal function for CSV like data
func MarshalCSV(records []interface{}, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	res := make(map[string]*layout.Table)

	numRecords := len(records)
	if numRecords <= 0 {
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

		table.Values = make([]interface{}, numRecords)
		table.RepetitionLevels = make([]int32, numRecords)
		table.DefinitionLevels = make([]int32, numRecords)
		for j := 0; j < numRecords; j++ {
			rec := records[j].([]interface{})[i]
			table.Values[j] = rec
			table.RepetitionLevels[j] = 0
			if rec == nil {
				table.DefinitionLevels[j] = 0
			} else {
				table.DefinitionLevels[j] = 1
			}
		}
	}
	return &res, nil
}
