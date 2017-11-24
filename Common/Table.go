package Common

import (
	"github.com/xitongsys/parquet-go/parquet"
)

//Table is the core data structure used to store the values
type Table struct {
	//Repetition type of the values: REQUIRED/OPTIONAL/REPEATED
	RepetitionType parquet.FieldRepetitionType
	//Parquet type
	Type parquet.Type
	//Path of this column
	Path []string
	//Maximum of definition levels
	MaxDefinitionLevel int32
	//Maximum of repetition levels
	MaxRepetitionLevel int32

	//Parquet values
	Values []interface{}
	//Definition Levels slice
	DefinitionLevels []int32
	//Repetition Levels slice
	RepetitionLevels []int32
}

//Merge several tables to one table(the first table)
func (table *Table) Merge(tables ...*Table) {
	ln := len(tables)
	if ln <= 0 {
		return
	}
	for i := 0; i < ln; i++ {
		table.Values = append(table.Values, tables[i].Values...)
		table.RepetitionLevels = append(table.RepetitionLevels, tables[i].RepetitionLevels...)
		table.DefinitionLevels = append(table.DefinitionLevels, tables[i].DefinitionLevels...)
	}
}
