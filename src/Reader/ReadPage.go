package Reader

import (
	. "Common"
	. "Compress"
	. "Layout"
	. "ParquetType"
	"bytes"
	"git.apache.org/thrift.git/lib/go/thrift"
)

func ReadPageHeader(thriftReader *thrift.TBufferedTransport) *parquet.PageHeader {
	protocol := thrift.NewTCompactProtocol(thriftReader)
	pageHeader := parquet.NewPageHeader()
	pageHeader.Read(protocol)
	return pageHeader
}

func ReadDataPageValues(bytesReader *bytes.Reader, encoding parquet.Encoding, dataType parquet.Type, cnt int32, bitWidth int32) []interface{} {
	if encoding == parquet.Encoding_PLAIN {
		return ReadPlain(bytesReader, dataType, cnt)

	} else if encoding == parquet.Encoding_PLAIN_DICTIONARY {
		b, _ := bytesReader.ReadByte()
		bitWidth = int32(b)
		return ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)

	} else if encoding == parquet.Encoding_RLE {
		return ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)

	} else if encoding == parquet.Encoding_BIT_PACKED {
	} else if encoding == parquet.Encoding_DELTA_BINARY_PACKED {
	} else if encoding == parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY {
	} else if encoding == parquet.Encoding_DELTA_BYTE_ARRAY {
	} else if encoding == parquet.Encoding_RLE_DICTIONARY {
	} else {
		log.Println("Error Encoding method")
	}
	log.Println("Encoding Not Supported Yet")
	return make([]interface{}, 0)
}

func ReadPage(thriftReader *thrift.TBufferedTransport, colMetaData *parquet.ColumnMetaData, schemaHandler *SchemaHandler) (*Page, int64) {
	pageHeader := ReadPageHeader(thriftReader)
	var page *Page
	compressedPageSize := pageHeader.GetCompressedPageSize()
	buf := make([]byte, compressedPageSize)
	thriftReader.Read(buf)
	codec := colMetaData.GetCodec()
	if codec == parquet.CompressionCodec_GZIP {
		buf = UncompressGzip(buf)
	} else if codec == parquet.CompressionCodec_SNAPPY {
		buf = UncompressSnappy(buf)
	} else if codec == parquet.CompressionCodec_UNCOMPRESSED {
		buf = buf
	} else {
		log.Panicln("Unsupported Codec: ", codec)
	}
	bytesReader := bytes.NewReader(buf)
	path := make([]string, 0)
	path = append(path, schemaHandler.GetRootName())
	path = append(path, colMetaData.GetPathInSchema()...)

	if pageHeader.GetType() == parquet.PageType_DATA_PAGE {
		page = NewDataPage()
		page.PageHeader = pageHeader
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)

		var repetitionLevels []interface{}
		if maxRepetitionLevel > 0 {
			bitWidth := BitNum(maxRepetitionLevel)

			repetitionLevels = ReadDataPageValues(bytesReader,
				pageHeader.DataPageHeader.GetRepetitionLevelEncoding(),
				parquet.Type_INT64,
				int32(pageHeader.DataPageHeader.GetNumValues()),
				bitWidth)

		} else {
			repetitionLevels = make([]Interface, pageHeader.DataPageHeader.GetNumValues())
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = int32(0)
			}
		}

		var definitionLevels []interface{}
		if maxDefinitionLevel > 0 {
			bitWidth := BitNum(maxDefinitionLevel)

			definitionLevels = ReadDataPageValues(bytesReader,
				pageHeader.DataPageHeader.GetDefinitionLevelEncoding(),
				parquet.Type_INT64, int32(pageHeader.DataPageHeader.GetNumValues()),
				bitWidth)

		} else {
			definitionLevels = make([]interface{}, pageHeader.DataPageHeader.GetNumValues())
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = int32(0)
			}
		}

		var numNulls int64 = 0
		for i := 0; i < len(definitionLevels); i++ {
			if int32(definitionLevels[i].(INT64)) != maxDefinitionLevel {
				numNULLs++
			}
		}

		var values []interface{}
		values = ReadDataPageValues(bytesReader,
			pageHeader.DataPageHeader.GetEncoding(),
			colMetaData.GetType(),
			int64(len(definitionLevels))-numNulls)

		table := new(Table)
		table.Path = path
		name := strings.Join(path, ".")
		table.Repetition_Type = schemaHandler.SchemaElements[name].GetRepetitionType()
		table.MaxRepetitionLevel = maxRepetitionLevel
		table.MaxDefinitionLevel = maxDefinitionLevel
		table.Values = make([]interface{}, len(definitionLevels))
		table.RepetitionLevels = make([]int32, len(definitionLevels))
		table.DefinitionLevels = make([]int32, len(definitionLevels))

		j := 0
		for i := 0; i < len(definitionLevels); i++ {
			dl, _ := definitionLevels[i].(int32)
			rl, _ := repetitionLevels[i].(int32)
			res.RepetitionLevels[i] = rl
			res.DefinitionLevels[i] = dl
			if res.DefinitionLevels[i] == maxDefinitionLevel {
				res.Values[i] = values[j]
				j++
			}
		}
		page.DataTable = table
		return page, len(definitionLevels)

	} else if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		page = NewDictPage()
		page.PageHeader = pageHeader
		table := new(Table)
		table.Path = path
		table.Values = ReadPlain(bytesReader,
			colMetaData.GetType(),
			pageHeader.DictionaryPageHeader().GetNumValues())

		return page, 0

	} else if pageHeader.GetType() == parquet.PageType_INDEX_PAGE {
	} else if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
	} else {
		log.Println("Error page type")
	}

	log.Println("Page Type Not Supported Yet")

	return nil, 0

}
