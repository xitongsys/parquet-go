package layout

import (
	"fmt"
	"testing"
)

func TestTable_Merge(t *testing.T) {
	tables := make([]*Table, 2)
	tables[0], tables[1] = new(Table), new(Table)
	tables[0].Values = []interface{}{int32(1), int32(2)}
	tables[1].Values = []interface{}{int32(3), int32(4)}
	tables[0].DefinitionLevels = []int32{0, 0}
	tables[1].DefinitionLevels = []int32{0, 0}
	tables[0].RepetitionLevels = []int32{0, 0}
	tables[1].RepetitionLevels = []int32{0, 0}

	tables[0].Merge(tables[1])
	if fmt.Sprintf("%v", tables[0].Values) != fmt.Sprintf("%v", []interface{}{int32(1), int32(2), int32(3), int32(4)}) ||
		fmt.Sprintf("%v", tables[0].DefinitionLevels) != fmt.Sprintf("%v", []int32{0, 0, 0, 0}) ||
		fmt.Sprintf("%v", tables[0].RepetitionLevels) != fmt.Sprintf("%v", []int32{0, 0, 0, 0}) {
		t.Errorf("Unexpected result: %v", tables)
	}
}

func TestTable_Pop(t *testing.T) {
	table := NewEmptyTable()
	table.Values = []interface{}{int32(1), int32(2)}
	table.DefinitionLevels = []int32{0, 0}
	table.RepetitionLevels = []int32{0, 0}

	actual := table.Pop(1)
	if fmt.Sprintf("%v", actual.Values) != fmt.Sprintf("%v", []interface{}{int32(1)}) ||
		fmt.Sprintf("%v", actual.DefinitionLevels) != fmt.Sprintf("%v", []int32{0}) ||
		fmt.Sprintf("%v", actual.RepetitionLevels) != fmt.Sprintf("%v", []int32{0}) {
		t.Errorf("Unexpected result: %v", actual)
	}
}
