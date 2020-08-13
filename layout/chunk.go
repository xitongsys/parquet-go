package layout

import (
	"github.com/syucream/parquet-go/common"
	"github.com/syucream/parquet-go/encoding"
	"github.com/syucream/parquet-go/parquet"
)

var (
	dataEncoding = []parquet.Encoding{
		parquet.Encoding_RLE,
		parquet.Encoding_BIT_PACKED,
		parquet.Encoding_PLAIN,
	}
	dictEncoding = []parquet.Encoding{
		parquet.Encoding_RLE,
		parquet.Encoding_BIT_PACKED,
		parquet.Encoding_PLAIN,
		parquet.Encoding_PLAIN_DICTIONARY,
		parquet.Encoding_RLE_DICTIONARY,
	}
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
	pT, cT := pages[0].Schema.Type, pages[0].Schema.ConvertedType
	funcTable := common.FindFuncTable(pT, cT)

	for i := 0; i < ln; i++ {
		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) + int64(len(pages[i].RawData)) - int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		minVal = common.Min(funcTable, minVal, pages[i].MinVal)
		maxVal = common.Max(funcTable, maxVal, pages[i].MaxVal)
	}

	chunk := new(Chunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = *pages[0].Schema.Type
	metaData.Encodings = dataEncoding
	metaData.Codec = pages[0].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[0].Path
	metaData.Statistics = parquet.NewStatistics()

	if maxVal != nil && minVal != nil {
		tmpBufMax := encoding.WritePlain([]interface{}{maxVal}, *pT)
		tmpBufMin := encoding.WritePlain([]interface{}{minVal}, *pT)
		if (cT != nil && *cT == parquet.ConvertedType_UTF8) ||
			(cT != nil && *cT == parquet.ConvertedType_DECIMAL && *pT == parquet.Type_BYTE_ARRAY) {
			tmpBufMax = tmpBufMax[4:]
			tmpBufMin = tmpBufMin[4:]
		}
		metaData.Statistics.Max = tmpBufMax
		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.MaxValue = tmpBufMax
		metaData.Statistics.MinValue = tmpBufMin
	}

	chunk.ChunkHeader.MetaData = metaData
	return chunk
}

//Convert several pages to one chunk with dict page first
func PagesToDictChunk(pages []*Page) *Chunk {
	if len(pages) < 2 {
		return nil
	}
	var numValues int64 = 0
	var totalUncompressedSize int64 = 0
	var totalCompressedSize int64 = 0

	var maxVal interface{} = pages[1].MaxVal
	var minVal interface{} = pages[1].MinVal
	pT, cT := pages[1].Schema.Type, pages[1].Schema.ConvertedType
	funcTable := common.FindFuncTable(pT, cT)

	for i := 0; i < len(pages); i++ {
		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else if pages[i].Header.DataPageHeaderV2 != nil {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) + int64(len(pages[i].RawData)) - int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		if i > 0 {
			minVal = common.Min(funcTable, minVal, pages[i].MinVal)
			maxVal = common.Max(funcTable, maxVal, pages[i].MaxVal)
		}
	}

	chunk := new(Chunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = *pages[1].Schema.Type
	metaData.Encodings = dictEncoding

	metaData.Codec = pages[1].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[1].Path
	metaData.Statistics = parquet.NewStatistics()

	if maxVal != nil && minVal != nil {
		tmpBufMax := encoding.WritePlain([]interface{}{maxVal}, *pT)
		tmpBufMin := encoding.WritePlain([]interface{}{minVal}, *pT)
		if (cT != nil && *cT == parquet.ConvertedType_UTF8) ||
			(cT != nil && *cT == parquet.ConvertedType_DECIMAL && *pT == parquet.Type_BYTE_ARRAY) {
			tmpBufMax = tmpBufMax[4:]
			tmpBufMin = tmpBufMin[4:]
		}
		metaData.Statistics.Max = tmpBufMax
		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.MaxValue = tmpBufMax
		metaData.Statistics.MinValue = tmpBufMin
	}

	chunk.ChunkHeader.MetaData = metaData
	return chunk
}
