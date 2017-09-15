package Reader

import (
	. "Layout"
	. "SchemaHandler"
	"git.apache.org/thrift.git/lib/go/thrift"
	"parquet"
)

func ReadChunk(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler, chunkHeader *parquet.ColumnChunk) *Chunk {
	chunk := new(Chunk)
	chunk.ChunkHeader = chunkHeader

	var readRows int64 = 0
	var numRows int64 = chunkHeader.MetaData.GetNumValues()
	for readRows < numRows {
		page, cnt := ReadPage(thriftReader, schemaHandler, chunkHeader.GetMetaData())
		chunk.Pages = append(chunk.Pages, page)
		readRows += cnt
	}
	return chunk
}
