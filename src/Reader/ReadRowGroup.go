package Reader

import (
	. "Layout"
	. "SchemaHandler"
	"os"
	"parquet"
)

func ReadRowGroup(file *os.File, schemaHandler *SchemaHandler, rowGroupHeader *parquet.RowGroup) *RowGroup {
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = rowGroupHeader
	for _, columnChunk := range rowGroupHeader.GetColumns() {

		offset := columnChunk.FileOffset
		thriftReader := ConvertToThriftReader(file, offset)

		chunk := ReadChunk(thriftReader, schemaHandler, columnChunk)
		rowGroup.Chunks = append(rowGroup.Chunks, chunk)
	}
	return rowGroup
}
