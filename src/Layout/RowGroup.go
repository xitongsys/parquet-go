package Layout

import (
	. "Common"
	"parquet"
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

func (rowGroup *RowGroup) RowGroupToTable() *Table {
	tabList := make([]*Table, 0)
	for _, chunk := range rowGroup.Chunks {
		for _, page := range chunk.Pages {
			tabList = append(tabList, page.DataTable)
		}
	}
	return MergeTable(tabList...)
}
