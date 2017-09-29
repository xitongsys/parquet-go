package ParquetHandler

import (
	. "Layout"
	"parquet"
)

func (self *ParquetHandler) ReadRowGroup(rowGroupHeader *parquet.RowGroup) *RowGroup {
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = rowGroupHeader
	for _, columnChunk := range rowGroupHeader.GetColumns() {

		offset := columnChunk.FileOffset
		thriftReader := ConvertToThriftReader(self.PFile, offset)

		chunk := self.ReadChunk(thriftReader, self.SchemaHandler, columnChunk)
		rowGroup.Chunks = append(rowGroup.Chunks, chunk)
	}
	return rowGroup
}
