package Common

import (
	"github.com/xitongsys/parquet-go/parquet"
)

//Table is the core data structure used to store the values
type Table struct {
	//Repetition type of the values: REQUIRED/OPTIONAL/REPEATED
	Repetition_Type parquet.FieldRepetitionType
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
