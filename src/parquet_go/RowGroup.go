package parquet_go

import (
	"parquet"
)

type RowGroup struct {
	Chunks []*Chunk
	RowGroupHeader *parquet.RowGroup
}

func NewRowGroup() *RowGroup{
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = parquet.NewRowGroup()
	return rowGroup
}

