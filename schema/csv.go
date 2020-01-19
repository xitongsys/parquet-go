package schema

import (
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
)

//Create a schema handler from CSV metadata
func NewSchemaHandlerFromMetadata(mds []string) *SchemaHandler {
	schemaList := make([]*parquet.SchemaElement, 0)
	infos := make([]*common.Tag, 0)

	rootSchema := parquet.NewSchemaElement()
	rootSchema.Name = "Parquet_go_root"
	rootNumChildren := int32(len(mds))
	rootSchema.NumChildren = &rootNumChildren
	rt := parquet.FieldRepetitionType_REQUIRED
	rootSchema.RepetitionType = &rt
	schemaList = append(schemaList, rootSchema)

	rootInfo := common.NewTag()
	rootInfo.InName = "Parquet_go_root"
	rootInfo.ExName = "parquet_go_root"
	rootInfo.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	infos = append(infos, rootInfo)

	for _, md := range mds {
		info := common.StringToTag(md)
		infos = append(infos, info)

		schema := parquet.NewSchemaElement()
		schema.Name = info.ExName
		numChildren := int32(0)
		schema.NumChildren = &numChildren
		rt := parquet.FieldRepetitionType_OPTIONAL
		schema.RepetitionType = &rt

		if t, err := parquet.TypeFromString(info.Type); err == nil {
			schema.Type = &t
			if info.Type == "FIXED_LEN_BYTE_ARRAY" {
				schema.TypeLength = &info.Length
			}
		} else {
			name := info.Type
			ct, _ := parquet.ConvertedTypeFromString(name)
			schema.ConvertedType = &ct
			if name == "INT_8" || name == "INT_16" || name == "INT_32" ||
				name == "UINT_8" || name == "UINT_16" || name == "UINT_32" ||
				name == "DATE" || name == "TIME_MILLIS" {
				schema.Type = parquet.TypePtr(parquet.Type_INT32)
			} else if name == "INT_64" || name == "UINT_64" ||
				name == "TIME_MICROS" || name == "TIMESTAMP_MICROS" || name == "TIMESTAMP_MILLIS" {
				schema.Type = parquet.TypePtr(parquet.Type_INT64)
			} else if name == "UTF8" {
				schema.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
			} else if name == "INTERVAL" {
				schema.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
				var ln int32 = 12
				schema.TypeLength = &ln
			} else if name == "DECIMAL" {
				if info.BaseType == parquet.Type_BYTE_ARRAY.String() {
					schema.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
				} else if info.BaseType == parquet.Type_FIXED_LEN_BYTE_ARRAY.String() {
					schema.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
					schema.TypeLength = &info.Length
				} else if info.BaseType == parquet.Type_INT32.String() {
					schema.Type = parquet.TypePtr(parquet.Type_INT32)
				} else if info.BaseType == parquet.Type_INT64.String() {
					schema.Type = parquet.TypePtr(parquet.Type_INT64)
				}
				schema.Scale = &info.Scale
				schema.Precision = &info.Precision
			}
		}
		schemaList = append(schemaList, schema)
	}
	res := NewSchemaHandlerFromSchemaList(schemaList)
	res.Infos = infos
	res.CreateInExMap()
	return res
}
