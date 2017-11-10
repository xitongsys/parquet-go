package Layout

import (
	. "github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/parquet"
)

//RowGroup stores the RowGroup in parquet file
type RowGroup struct {
	Chunks         []*Chunk
	RowGroupHeader *parquet.RowGroup
}

//Create a RowGroup
func NewRowGroup() *RowGroup {
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = parquet.NewRowGroup()
	return rowGroup
}

//Convert a RowGroup to table map
func (rowGroup *RowGroup) RowGroupToTableMap() *map[string]*Table {
	tableMap := make(map[string]*Table, 0)
	for _, chunk := range rowGroup.Chunks {
		pathStr := ""
		for _, page := range chunk.Pages {
			if pathStr == "" {
				pathStr = PathToStr(page.DataTable.Path)
			}
			if _, ok := tableMap[pathStr]; ok {
				tableMap[pathStr] = MergeTable(tableMap[pathStr], page.DataTable)
			} else {
				tableMap[pathStr] = page.DataTable
			}
		}
	}
	return &tableMap
}
