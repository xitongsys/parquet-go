package Layout

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/ParquetEncoding"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
)

//Chunk stores the ColumnChunk in parquet file
type Chunk struct {
	Pages       []*Page
	ChunkHeader *parquet.ColumnChunk
}

//Convert several pages to one chunk
func PagesToChunk(pages []*Page) *Chunk {
	ln := len(pages)
	var numValues int64 = 0
	var totalUncompressedSize int64 = 0
	var totalCompressedSize int64 = 0

	var maxVal interface{} = pages[0].MaxVal
	var minVal interface{} = pages[0].MinVal

	for i := 0; i < ln; i++ {
		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) + int64(len(pages[i].RawData)) - int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		maxVal = Common.Max(maxVal, pages[i].MaxVal)
		minVal = Common.Min(minVal, pages[i].MinVal)
	}

	chunk := new(Chunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = pages[0].DataType
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_BIT_PACKED)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN)
	//metaData.Encodings = append(metaData.Encodings, parquet.Encoding_DELTA_BINARY_PACKED)
	metaData.Codec = pages[0].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[0].Path[1:]
	metaData.Statistics = parquet.NewStatistics()

	tmpBufMax := ParquetEncoding.WritePlain([]interface{}{maxVal})
	tmpBufMin := ParquetEncoding.WritePlain([]interface{}{minVal})
	name := reflect.TypeOf(maxVal).Name()

	if name == "UTF8" || name == "DECIMAL" {
		tmpBufMax = tmpBufMax[4:]
		tmpBufMin = tmpBufMin[4:]
	}
	metaData.Statistics.Max = tmpBufMax
	metaData.Statistics.Min = tmpBufMin

	chunk.ChunkHeader.MetaData = metaData
	return chunk
}

//Decode a dict chunk
func DecodeDictChunk(chunk *Layout.Chunk) {
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
func ReadChunk(thriftReader *thrift.TBufferedTransport, schemaHandler *SchemaHandler.SchemaHandler, chunkHeader *parquet.ColumnChunk) *Layout.Chunk {
	chunk := new(Layout.Chunk)
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
