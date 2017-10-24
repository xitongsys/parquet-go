package CSVWriter

import (
	. "github.com/xitongsys/parquet-go/Common"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/parquet"
)

type MetadataType struct {
	Type       string
	Name       string
	TypeLength int32
	Scale      int32
	Precision  int32
}

type CSVWriterHandler struct {
	SchemaHandler *SchemaHandler
	NP            int64
	Footer        *parquet.FileMetaData
	RowGroups     []*RowGroups

	PFile ParquetFile

	PageSize     int64
	RowGroupSize int64
	Offset       int64
	Record       [][]string
	Metadata     []MetadataType
	RecAveSize   int64
	Size         int64
}

func NewSchemaHandlerFromMetadata(mds []MetadataType) *SchemaHandler {
	schemaList := make([]*parquet.SchemaElement)

	rootSchema := parquet.NewSchemaElement()
	rootSchema.Name = "parquet-go-root"
	rootNumChildren := len(mds)
	rootSchema.NumChildren = &rootNumChildren
	schemaList = append(schemaList, rootSchema)

	for _, md := range mds {
		schema := parquet.NewSchemaElement()
		schema.Name = md.Name
		numChildren := 0
		schema.NumChildren = &numChildren

		if IsBaseType(md.Type) {
			t := NameToBaseType(md.Type)
			schema.Type = &t
			if md.Type == "FIXED_LEN_BYTE_ARRAY" {
				schema.TypeLength = &schema.TypeLength
			}

		} else {
			if name == "INT_8" || name == "INT_16" || name == "INT_32" ||
				name == "UINT_8" || name == "UINT_16" || name == "UINT_32" ||
				name == "DATE" || name == "TIME_MILLIS" {
				t := parquet.Type_INT32
				ct := ParquetType.NameToConvertedType(name)
				schema.Type = &t
				schema.ConvertedType = &ct
			} else if name == "INT_64" || name == "UINT_64" ||
				name == "TIME_MICROS" || name == "TIMESTAMP_MICROS" {
				t := parquet.Type_INT64
				ct := ParquetType.NameToConvertedType(name)
				schema.Type = &t
				schema.ConvertedType = &ct
			} else if name == "UTF8" {
				t := parquet.Type_BYTE_ARRAY
				ct := ParquetType.NameToConvertedType(name)
				schema.Type = &t
				schema.ConvertedType = &ct
			} else if name == "INTERVAL" {
				t := parquet.Type_FIXED_LEN_BYTE_ARRAY
				ct := ParquetType.NameToConvertedType(name)
				var ln int32 = 12
				schema.Type = &t
				schema.ConvertedType = &ct
				schema.TypeLength = &ln
			} else if name == "DECIMAL" {
				ct := ParquetType.NameToConvertedType(name)
				t := ParquetType.NameToBaseType("BYTE_ARRAY")
				scale := md.Scale
				precision := md.Precision

				schema.Type = &t
				schema.ConvertedType = &ct
				schema.Scale = &scale
				schema.Precision = &precision

			}
		}

		schemaList = append(schemaList, schema)
	}

	return NewSchemaHandlerFromSchemaList(schemaList)

}

func NewCSVWriterHandler() *CSVWriterHandler {
	res := new(CSVWriterHandler)
	res.NP = 1
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	return res
}

func (self *CSVWriterHandler) WriteInit(md []string, pfile ParquetFile, np int64, recordAveSize int64) {
	self.Metadata = md
	self.PFile = pfile
	self.NP = np
	self.RecAveSize = recordAveSize
	self.Footer = parquet.NewFileMetaData()
	self.Footer.Version = 1
	self.Offset = 4
	self.PFile.Write([]byte("PAR1"))
}
