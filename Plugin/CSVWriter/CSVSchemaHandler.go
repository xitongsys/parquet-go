package CSVWriter

import (
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"log"
)

//Create a schema handler from CSV metadata
func NewSchemaHandlerFromMetadata(mds []string) *SchemaHandler.SchemaHandler {
	schemaList := make([]*parquet.SchemaElement, 0)

	infos := make([]map[string]interface{}, 0)

	rootSchema := parquet.NewSchemaElement()
	rootSchema.Name = "parquet_go_root"
	rootNumChildren := int32(len(mds))
	rootSchema.NumChildren = &rootNumChildren
	rt := parquet.FieldRepetitionType(-1)
	rootSchema.RepetitionType = &rt
	schemaList = append(schemaList, rootSchema)

	for _, md := range mds {
		info := Common.TagToMap(md)
		infos = append(infos, info)

		schema := parquet.NewSchemaElement()
		schema.Name = info["exname"].(string)
		numChildren := int32(0)
		schema.NumChildren = &numChildren
		rt := parquet.FieldRepetitionType(1)
		schema.RepetitionType = &rt

		if t, err := parquet.TypeFromString(info["type"].(string)); err == nil {
			schema.Type = &t
			if info["type"].(string) == "FIXED_LEN_BYTE_ARRAY" {
				ln := info["length"].(int32)
				schema.TypeLength = &ln
			}

		} else {
			name := info["type"].(string)
			ct, _ := parquet.ConvertedTypeFromString(info["type"].(string))
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
				scale := info["scale"].(int32)
				precision := info["precision"].(int32)
				schema.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
				schema.Scale = &scale
				schema.Precision = &precision

			}
		}

		schemaList = append(schemaList, schema)
	}

	log.Println("=========", schemaList)
	res := SchemaHandler.NewSchemaHandlerFromSchemaList(schemaList)
	res.Infos = infos
	return res

}
