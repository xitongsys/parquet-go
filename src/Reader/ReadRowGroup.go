package Reader

import (
	. "Layout"
	"git.apache.org/thrift.git/lib/go/thrift"
	"parquet"
)

func ReadRowGroup(file *os.file, schemaHandler *SchemaHandler, rowGroupHeader *parquet.RowGroup) *RowGroup {
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = rowGroupHeader
	for _, columnChunk := range rowGroupHeader.GetColumns() {

		offset := columnChunk.FileOffset
		thriftReader := ConvertToThriftReader(file, offset)

		chunk = ReadChunk(thriftReader, schemaHandler, columnChunk)
		rowGroup.Chunks = append(rowGroup.Chunks, chunk)
	}

	return rowGroup
}
