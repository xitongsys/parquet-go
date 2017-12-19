package CSVWriter

import (
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

//Marshal function for CSV like data
func MarshalCSV(records [][]interface{}, bgn int, end int, schemaHandler *SchemaHandler.SchemaHandler) *map[string]*Layout.Table {
	res := make(map[string]*Layout.Table)
	if ln := len(records); ln <= 0 {
		return &res
	}

	for i := 0; i < len(records[0]); i++ {
		pathStr := schemaHandler.GetRootName() + "." + schemaHandler.Infos[i]["exname"].(string)
		res[pathStr] = Layout.NewEmptyTable()
		res[pathStr].Path = Common.StrToPath(pathStr)
		res[pathStr].MaxDefinitionLevel = 1
		res[pathStr].MaxRepetitionLevel = 0
		res[pathStr].RepetitionType = parquet.FieldRepetitionType_OPTIONAL
		res[pathStr].Type = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]].GetType()
		res[pathStr].Info = schemaHandler.Infos[i]

		for j := bgn; j < end; j++ {
			rec := records[j][i]
			res[pathStr].Values = append(res[pathStr].Values, rec)
			res[pathStr].RepetitionLevels = append(res[pathStr].RepetitionLevels, 0)

			if rec == nil {
				res[pathStr].DefinitionLevels = append(res[pathStr].DefinitionLevels, 0)
			} else {
				res[pathStr].DefinitionLevels = append(res[pathStr].DefinitionLevels, 1)
			}
		}
	}
	return &res
}
