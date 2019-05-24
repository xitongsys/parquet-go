package layout

import (
	"fmt"
	"testing"
)

func TestMergeTable(t *testing.T) {
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
		t.Errorf("MergeTable err")
	}
}
