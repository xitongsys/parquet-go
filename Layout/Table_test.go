package Layout

import (
	"fmt"
	"testing"

	. "github.com/xitongsys/parquet-go/ParquetType"
)

func TestMergeTable(t *testing.T) {
	tables := make([]*Table, 2)
	tables[0], tables[1] = new(Table), new(Table)
	tables[0].Values = []interface{}{INT32(1), INT32(2)}
	tables[1].Values = []interface{}{INT32(3), INT32(4)}
	tables[0].DefinitionLevels = []int32{0, 0}
	tables[1].DefinitionLevels = []int32{0, 0}
	tables[0].RepetitionLevels = []int32{0, 0}
	tables[1].RepetitionLevels = []int32{0, 0}

	tables[0].Merge(tables[1])
	if fmt.Sprintf("%v", tables[0].Values) != fmt.Sprintf("%v", []interface{}{INT32(1), INT32(2), INT32(3), INT32(4)}) ||
		fmt.Sprintf("%v", tables[0].DefinitionLevels) != fmt.Sprintf("%v", []int32{0, 0, 0, 0}) ||
		fmt.Sprintf("%v", tables[0].RepetitionLevels) != fmt.Sprintf("%v", []int32{0, 0, 0, 0}) {
		t.Errorf("MergeTable err")
	}
}
