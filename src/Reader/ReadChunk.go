package Reader

import (
	. "Layout"
	. "ParquetType"
	. "SchemaHandler"
	"git.apache.org/thrift.git/lib/go/thrift"
	"parquet"
)

func DecodeDictChunk(chunk *Chunk) {
	dictPage := chunk.Pages[0]
	numPages := len(chunk.Pages)
	for i := 1; i < numPages; i++ {
		numValues := len(chunk.Pages[i].DataTable.Values)
		for j := 0; j < numValues; j++ {
			if chunk.Pages[i].DataTable.Values[j] != nil {
				index := chunk.Pages[i].DataTable.Values[j].(INT64)
				chunk.Pages[i].DataTable.Values[j] = dictPage.DataTable.Values[index]
			}
		}
	}
}

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

	if len(chunk.Pages) > 0 && chunk.Pages[0].Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		DecodeDictChunk(chunk)
	}
	return chunk
}
