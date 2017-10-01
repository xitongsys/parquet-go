package ParquetHandler

import (
	. "github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/parquet"
)

func (self *ParquetHandler) ReadRowGroup(rowGroupHeader *parquet.RowGroup) *RowGroup {
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = rowGroupHeader
	for _, columnChunk := range rowGroupHeader.GetColumns() {

		offset := columnChunk.FileOffset
		if columnChunk.FilePath != nil {
			self.PFile.Open(*columnChunk.FilePath)
		}

		thriftReader := ConvertToThriftReader(self.PFile, offset)

		chunk := self.ReadChunk(thriftReader, self.SchemaHandler, columnChunk)
		rowGroup.Chunks = append(rowGroup.Chunks, chunk)
	}
	return rowGroup
}
