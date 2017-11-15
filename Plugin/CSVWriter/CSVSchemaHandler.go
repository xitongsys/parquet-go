package CSVWriter

import (
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

//CSV metadata
type MetadataType struct {
	Type       string
	Name       string
	TypeLength int32
	Scale      int32
	Precision  int32
}

//Create a schema handler from CSV metadata
func NewSchemaHandlerFromMetadata(mds []MetadataType) *SchemaHandler.SchemaHandler {
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

		if t, err := parquet.TypeFromString(md.Type); err == nil {
			schema.Type = &t
			if md.Type == "FIXED_LEN_BYTE_ARRAY" {
				schema.TypeLength = &md.TypeLength
			}

		} else {
			name := md.Type
			ct, _ := parquet.ConvertedTypeFromString(name)
			schema.ConvertedType = &ct
			if name == "INT_8" || name == "INT_16" || name == "INT_32" ||
				name == "UINT_8" || name == "UINT_16" || name == "UINT_32" ||
				name == "DATE" || name == "TIME_MILLIS" {
				schema.Type = parquet.TypePtr(parquet.Type_INT32)
			} else if name == "INT_64" || name == "UINT_64" ||
				name == "TIME_MICROS" || name == "TIMESTAMP_MICROS" {
				schema.Type = parquet.TypePtr(parquet.Type_INT64)
			} else if name == "UTF8" {
				schema.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
			} else if name == "INTERVAL" {
				schema.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
				var ln int32 = 12
				schema.TypeLength = &ln
			} else if name == "DECIMAL" {
				scale := md.Scale
				precision := md.Precision
				schema.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
				schema.Scale = &scale
				schema.Precision = &precision

			}
		}

		schemaList = append(schemaList, schema)
	}

	return SchemaHandler.NewSchemaHandlerFromSchemaList(schemaList)

}
