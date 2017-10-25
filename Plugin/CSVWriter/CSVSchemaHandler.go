package CSVWriter

import (
	. "github.com/xitongsys/parquet-go/ParquetType"
	. "github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

type MetadataType struct {
	Type       string
	Name       string
	TypeLength int32
	Scale      int32
	Precision  int32
}

func NewSchemaHandlerFromMetadata(mds []MetadataType) *SchemaHandler {
	schemaList := make([]*parquet.SchemaElement, 0)

	rootSchema := parquet.NewSchemaElement()
	rootSchema.Name = "parquet-go-root"
	rootNumChildren := int32(len(mds))
	rootSchema.NumChildren = &rootNumChildren
	rt := parquet.FieldRepetitionType(-1)
	rootSchema.RepetitionType = &rt
	schemaList = append(schemaList, rootSchema)

	for _, md := range mds {
		schema := parquet.NewSchemaElement()
		schema.Name = md.Name
		numChildren := int32(0)
		schema.NumChildren = &numChildren
		rt := parquet.FieldRepetitionType(1)
		schema.RepetitionType = &rt

		if IsBaseType(md.Type) {
			t := NameToBaseType(md.Type)
			schema.Type = &t
			if md.Type == "FIXED_LEN_BYTE_ARRAY" {
				schema.TypeLength = &md.TypeLength
			}

		} else {
			name := md.Type
			if name == "INT_8" || name == "INT_16" || name == "INT_32" ||
				name == "UINT_8" || name == "UINT_16" || name == "UINT_32" ||
				name == "DATE" || name == "TIME_MILLIS" {
				t := parquet.Type_INT32
				ct := NameToConvertedType(name)
				schema.Type = &t
				schema.ConvertedType = &ct
			} else if name == "INT_64" || name == "UINT_64" ||
				name == "TIME_MICROS" || name == "TIMESTAMP_MICROS" {
				t := parquet.Type_INT64
				ct := NameToConvertedType(name)
				schema.Type = &t
				schema.ConvertedType = &ct
			} else if name == "UTF8" {
				t := parquet.Type_BYTE_ARRAY
				ct := NameToConvertedType(name)
				schema.Type = &t
				schema.ConvertedType = &ct
			} else if name == "INTERVAL" {
				t := parquet.Type_FIXED_LEN_BYTE_ARRAY
				ct := NameToConvertedType(name)
				var ln int32 = 12
				schema.Type = &t
				schema.ConvertedType = &ct
				schema.TypeLength = &ln
			} else if name == "DECIMAL" {
				ct := NameToConvertedType(name)
				t := NameToBaseType("BYTE_ARRAY")
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
