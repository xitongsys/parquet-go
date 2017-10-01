package Layout

import (
	. "github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/parquet"
)

type RowGroup struct {
	Chunks         []*Chunk
	RowGroupHeader *parquet.RowGroup
}

func NewRowGroup() *RowGroup {
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = parquet.NewRowGroup()
	return rowGroup
}

func (rowGroup *RowGroup) RowGroupToTableMap() *map[string]*Table {
	tableMap := make(map[string]*Table, 0)
	for _, chunk := range rowGroup.Chunks {
		for _, page := range chunk.Pages {
			pathStr := PathToStr(page.DataTable.Path)
			if _, ok := tableMap[pathStr]; ok {
				tableMap[pathStr] = MergeTable(tableMap[pathStr], page.DataTable)
			} else {
				tableMap[pathStr] = page.DataTable
			}
		}
	}
	return &tableMap
}
