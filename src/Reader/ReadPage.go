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

func ReadPage(thriftReader *thrift.TBufferedTransport, colMetaData *parquet.ColumnMetaData, schemaHandler *SchemaHandler) *Page {
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
			repetitionLevels = ReadValues()
		} else {
			repetitionLevels = make([]Interface, pageHeader.DataPageHeader.GetNumValues())
			for i := 0; i < len(repetitionLevels); i++ {
				repetitionLevels[i] = int32(0)
			}
		}

		var definitionLevels []interface{}
		if maxDefinitionLevel > 0 {
			bitWidth := BitNum(maxDefinitionLevel)
			definitionLevels = ReadValues()
		} else {
			definitionLevels = make([]interface{}, pageHeader.DataPageHeader.GetNumValues())
			for i := 0; i < len(definitionLevels); i++ {
				definitionLevels[i] = int32(0)
			}
		}

		var values []interface{}
		values = ReadValues()

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

	} else if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		page = NewDictPage()
		page.PageHeader = pageHeader
	}

}
