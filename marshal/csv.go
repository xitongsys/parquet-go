package marshal

import (
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/parquet"
)

//Marshal function for CSV like data
func MarshalCSV(records []interface{}, bgn int, end int, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	res := make(map[string]*layout.Table)
	if ln := len(records); ln <= 0 {
		return &res, nil
	}

	for i := 0; i < len(records[0].([]interface{})); i++ {
		pathStr := schemaHandler.GetRootInName() + "." + schemaHandler.Infos[i+1].InName
		res[pathStr] = layout.NewEmptyTable()
		res[pathStr].Path = common.StrToPath(pathStr)
		res[pathStr].MaxDefinitionLevel = 1
		res[pathStr].MaxRepetitionLevel = 0
		res[pathStr].RepetitionType = parquet.FieldRepetitionType_OPTIONAL
		res[pathStr].Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
		res[pathStr].Info = schemaHandler.Infos[i+1]

		for j := bgn; j < end; j++ {
			rec := records[j].([]interface{})[i]
			res[pathStr].Values = append(res[pathStr].Values, rec)
			res[pathStr].RepetitionLevels = append(res[pathStr].RepetitionLevels, 0)

			if rec == nil {
				res[pathStr].DefinitionLevels = append(res[pathStr].DefinitionLevels, 0)
			} else {
				res[pathStr].DefinitionLevels = append(res[pathStr].DefinitionLevels, 1)
			}
		}
	}
	return &res, nil
}
