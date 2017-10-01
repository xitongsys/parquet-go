package Common

import (
	"github.com/xitongsys/parquet-go/parquet"
)

type Table struct {
	Repetition_Type    parquet.FieldRepetitionType
	Type               parquet.Type
	Path               []string
	MaxDefinitionLevel int32
	MaxRepetitionLevel int32

	Values           []interface{}
	DefinitionLevels []int32
	RepetitionLevels []int32
}

func MergeTable(tables ...*Table) *Table {
	ln := len(tables)
	if ln <= 0 {
		return nil
	}
	for i := 1; i < ln; i++ {
		tables[0].Values = append(tables[0].Values, tables[i].Values...)
		tables[0].RepetitionLevels = append(tables[0].RepetitionLevels, tables[i].RepetitionLevels...)
		tables[0].DefinitionLevels = append(tables[0].DefinitionLevels, tables[i].DefinitionLevels...)
	}
	return tables[0]
}
