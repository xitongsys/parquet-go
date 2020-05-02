package layout

import (
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
)

func NewTableFromTable(src *Table) *Table {
	if src == nil {
		return nil
	}
	table := new(Table)
	table.Schema = src.Schema
	table.Path = append(table.Path, src.Path...)
	table.MaxDefinitionLevel = 0
	table.MaxRepetitionLevel = 0
	table.Info = src.Info
	return table
}

func NewEmptyTable() *Table {
	table := new(Table)
	table.Info = common.NewTag()
	return table
}

//Table is the core data structure used to store the values
type Table struct {
	//Repetition type of the values: REQUIRED/OPTIONAL/REPEATED
	RepetitionType parquet.FieldRepetitionType
	//Schema
	Schema *parquet.SchemaElement
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

	//Tag info
	Info *common.Tag
}

//Merge several tables to one table(the first table)
func (self *Table) Merge(tables ...*Table) {
	ln := len(tables)
	if ln <= 0 {
		return
	}
	for i := 0; i < ln; i++ {
		if tables[i] == nil {
			continue
		}
		self.Values = append(self.Values, tables[i].Values...)
		self.RepetitionLevels = append(self.RepetitionLevels, tables[i].RepetitionLevels...)
		self.DefinitionLevels = append(self.DefinitionLevels, tables[i].DefinitionLevels...)
		if tables[i].MaxDefinitionLevel > self.MaxDefinitionLevel {
			self.MaxDefinitionLevel = tables[i].MaxDefinitionLevel
		}
		if tables[i].MaxRepetitionLevel > self.MaxRepetitionLevel {
			self.MaxRepetitionLevel = tables[i].MaxRepetitionLevel
		}
	}
}

func (self *Table) Pop(numRows int64) *Table {
	res := NewTableFromTable(self)
	endIndex := int64(0)
	ln := int64(len(self.Values))
	i, num := int64(0), int64(-1)
	for i = 0; i < ln; i++ {
		if self.RepetitionLevels[i] == 0 {
			num++
			if num >= numRows {
				break
			}
		}
		if res.MaxRepetitionLevel < self.RepetitionLevels[i] {
			res.MaxRepetitionLevel = self.RepetitionLevels[i]
		}
		if res.MaxDefinitionLevel < self.DefinitionLevels[i] {
			res.MaxDefinitionLevel = self.DefinitionLevels[i]
		}
	}
	endIndex = i

	res.RepetitionLevels = self.RepetitionLevels[:endIndex]
	res.DefinitionLevels = self.DefinitionLevels[:endIndex]
	res.Values = self.Values[:endIndex]

	self.RepetitionLevels = self.RepetitionLevels[endIndex:]
	self.DefinitionLevels = self.DefinitionLevels[endIndex:]
	self.Values = self.Values[endIndex:]

	return res
}
