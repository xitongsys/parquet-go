package layout

import (
	"github.com/syucream/parquet-go/parquet"
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
