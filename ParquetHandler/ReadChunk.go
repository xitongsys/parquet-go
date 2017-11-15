package ParquetHandler

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

//Decode a dict chunk
func (self *ParquetHandler) DecodeDictChunk(chunk *Layout.Chunk) {
	dictPage := chunk.Pages[0]
	numPages := len(chunk.Pages)
	for i := 1; i < numPages; i++ {
		numValues := len(chunk.Pages[i].DataTable.Values)
		for j := 0; j < numValues; j++ {
			if chunk.Pages[i].DataTable.Values[j] != nil {
				index := chunk.Pages[i].DataTable.Values[j].(ParquetType.INT64)
				chunk.Pages[i].DataTable.Values[j] = dictPage.DataTable.Values[index]
			}
		}
	}
	chunk.Pages = chunk.Pages[1:] // delete the head dict page
}

//Read one chunk from parquet file
func (self *ParquetHandler) ReadChunk(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler.SchemaHandler, chunkHeader *parquet.ColumnChunk) *Layout.Chunk {
	chunk := new(Layout.Chunk)
	chunk.ChunkHeader = chunkHeader

	var readRows int64 = 0
	var numRows int64 = chunkHeader.MetaData.GetNumValues()
	for readRows < numRows {
		page, cnt := self.ReadPage(thriftReader, schemaHandler, chunkHeader.GetMetaData())
		chunk.Pages = append(chunk.Pages, page)
		readRows += cnt
	}

	if len(chunk.Pages) > 0 && chunk.Pages[0].Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		self.DecodeDictChunk(chunk)
	}
	return chunk
}
