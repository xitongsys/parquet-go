package Reader

import (
	. "Layout"
	"git.apache.org/thrift.git/lib/go/thrift"
)

func ReadChunk(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler, chunkHeader *parquet.ColumnChunk, numRows int64) *Chunk {
	chunk = new(Chunk)
	chunk.ChunkHeader = chunkHeader

	var readRows int64 = 0
	for readRows < numRows {
		page, cnt := ReadPage(thriftReader, schemaHandler, chunkHeader.GetMetaData())
		chunk.Pages = append(chunk.Pages, page)
		readRows += cnt
	}
	return chunk
}
