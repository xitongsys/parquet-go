package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewSchemaHandlerFromStruct_invalid_tag(t *testing.T) {
	_, err := NewSchemaHandlerFromStruct(new(struct {
		Id int32 `parquet:"foo=bar, type=INT32"`
	}))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "unrecognized tag 'foo'")
}

func Test_NewSchemaHandlerFromStruct_invalid_type(t *testing.T) {
	_, err := NewSchemaHandlerFromStruct(new(struct {
		Name string `parquet:"name=name, type=UTF8"`
	}))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "field [Name] with type [UTF8]: not a valid Type strin")
}

func Test_NewSchemaHandlerFromStruct_ignore(t *testing.T) {
	schema, err := NewSchemaHandlerFromStruct(new(struct {
		Id   int32 `parquet:"name=id, type=INT32, ConvertedType=INT_32"`
		Name string
	}))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(schema.SchemaElements))
	assert.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
	assert.Equal(t, "Id", schema.SchemaElements[1].Name)
	assert.Equal(t, "INT32", schema.SchemaElements[1].Type.String())
	assert.Equal(t, "REQUIRED", schema.SchemaElements[1].RepetitionType.String())
	assert.True(t, schema.SchemaElements[1].LogicalType.IsSetINTEGER())
	assert.Equal(t, "INT_32", schema.SchemaElements[1].ConvertedType.String())
}

func Test_NewSchemaHandlerFromStruct_pointer(t *testing.T) {
	schema, err := NewSchemaHandlerFromStruct(new(struct {
		Name *string `parquet:"name=name, type=BYTE_ARRAY, ConvertedType=UTF8"`
	}))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(schema.SchemaElements))
	assert.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
	assert.Equal(t, "Name", schema.SchemaElements[1].Name)
	assert.Equal(t, "BYTE_ARRAY", schema.SchemaElements[1].Type.String())
	assert.Equal(t, "OPTIONAL", schema.SchemaElements[1].RepetitionType.String())
	assert.True(t, schema.SchemaElements[1].LogicalType.IsSetSTRING())
	assert.Equal(t, "UTF8", schema.SchemaElements[1].ConvertedType.String())
}

func Test_NewSchemaHandlerFromStruct_map_bad(t *testing.T) {
	_, err := NewSchemaHandlerFromStruct(new(struct {
		MapField map[string]*int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
	}))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "field [Value] with type []: not a valid Type string")

	_, err = NewSchemaHandlerFromStruct(new(struct {
		MapField map[string]*int32 `parquet:"name=map, type=MAP, convertedtype=MAP, valuetype=INT32"`
	}))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "field [Key] with type []: not a valid Type string")
}

func Test_NewSchemaHandlerFromStruct_map_good(t *testing.T) {
	schema, err := NewSchemaHandlerFromStruct(new(struct {
		MapField1 map[string]*int32  `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
		MapField2 *map[*string]int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	}))
	assert.Nil(t, err)
	assert.Equal(t, 9, len(schema.SchemaElements))
	assert.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
	assert.Equal(t, "MapField1", schema.SchemaElements[1].Name)
	assert.Equal(t, "Key_value", schema.SchemaElements[2].Name)
	assert.Equal(t, "Key", schema.SchemaElements[3].Name)
	assert.Equal(t, "Value", schema.SchemaElements[4].Name)
	assert.Equal(t, "MapField2", schema.SchemaElements[5].Name)
	assert.Equal(t, "Key_value", schema.SchemaElements[6].Name)
	assert.Equal(t, "Key", schema.SchemaElements[7].Name)
	assert.Equal(t, "Value", schema.SchemaElements[8].Name)

	assert.Equal(t, "REQUIRED", schema.SchemaElements[1].RepetitionType.String())
	assert.Equal(t, int32(1), schema.SchemaElements[1].GetNumChildren())
	assert.Nil(t, schema.SchemaElements[1].Type)
	assert.Equal(t, "MAP", schema.SchemaElements[1].ConvertedType.String())

	assert.Equal(t, "REPEATED", schema.SchemaElements[2].RepetitionType.String())
	assert.Equal(t, int32(2), schema.SchemaElements[2].GetNumChildren())
	assert.Nil(t, schema.SchemaElements[2].Type)
	assert.Equal(t, "MAP_KEY_VALUE", schema.SchemaElements[2].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema.SchemaElements[3].RepetitionType.String())
	assert.Equal(t, "BYTE_ARRAY", schema.SchemaElements[3].Type.String())
	assert.Equal(t, "UTF8", schema.SchemaElements[3].ConvertedType.String())

	assert.Equal(t, "OPTIONAL", schema.SchemaElements[4].RepetitionType.String())
	assert.Equal(t, "INT32", schema.SchemaElements[4].Type.String())
	assert.Nil(t, schema.SchemaElements[4].ConvertedType)

	assert.Equal(t, "OPTIONAL", schema.SchemaElements[5].RepetitionType.String())
	assert.Equal(t, int32(1), schema.SchemaElements[5].GetNumChildren())
	assert.Nil(t, schema.SchemaElements[5].Type)
	assert.Equal(t, "MAP", schema.SchemaElements[5].ConvertedType.String())

	assert.Equal(t, "REPEATED", schema.SchemaElements[6].RepetitionType.String())
	assert.Equal(t, int32(2), schema.SchemaElements[6].GetNumChildren())
	assert.Nil(t, schema.SchemaElements[6].Type)
	assert.Equal(t, "MAP_KEY_VALUE", schema.SchemaElements[6].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema.SchemaElements[7].RepetitionType.String())
	assert.Equal(t, "BYTE_ARRAY", schema.SchemaElements[7].Type.String())
	assert.Equal(t, "UTF8", schema.SchemaElements[7].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema.SchemaElements[8].RepetitionType.String())
	assert.Equal(t, "INT32", schema.SchemaElements[8].Type.String())
	assert.Nil(t, schema.SchemaElements[8].ConvertedType)
}

func Test_NewSchemaHandlerFromStruct_list_bad(t *testing.T) {
	_, err := NewSchemaHandlerFromStruct(new(struct {
		ListField *[]string `parquet:"name=list, type=LIST, convertedtype=LIST"`
	}))
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "field [Element] with type []: not a valid Type string")
}

func Test_NewSchemaHandlerFromStruct_list_good_list(t *testing.T) {
	schema, err := NewSchemaHandlerFromStruct(new(struct {
		ListField1 *[]string `parquet:"name=list, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		ListField2 []*string `parquet:"name=list, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	}))
	assert.Nil(t, err)
	assert.Equal(t, 7, len(schema.SchemaElements))
	assert.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
	assert.Equal(t, "ListField1", schema.SchemaElements[1].Name)
	assert.Equal(t, "List", schema.SchemaElements[2].Name)
	assert.Equal(t, "Element", schema.SchemaElements[3].Name)
	assert.Equal(t, "ListField2", schema.SchemaElements[4].Name)
	assert.Equal(t, "List", schema.SchemaElements[5].Name)
	assert.Equal(t, "Element", schema.SchemaElements[6].Name)

	assert.Equal(t, "OPTIONAL", schema.SchemaElements[1].RepetitionType.String())
	assert.Equal(t, int32(1), schema.SchemaElements[1].GetNumChildren())
	assert.Nil(t, schema.SchemaElements[1].Type)
	assert.Equal(t, "LIST", schema.SchemaElements[1].ConvertedType.String())

	assert.Equal(t, "REPEATED", schema.SchemaElements[2].RepetitionType.String())
	assert.Equal(t, int32(1), schema.SchemaElements[2].GetNumChildren())
	assert.Nil(t, schema.SchemaElements[2].Type)
	assert.Nil(t, schema.SchemaElements[2].ConvertedType)

	assert.Equal(t, "REQUIRED", schema.SchemaElements[3].RepetitionType.String())
	assert.Equal(t, "BYTE_ARRAY", schema.SchemaElements[3].Type.String())
	assert.Equal(t, "UTF8", schema.SchemaElements[3].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema.SchemaElements[4].RepetitionType.String())
	assert.Equal(t, int32(1), schema.SchemaElements[4].GetNumChildren())
	assert.Nil(t, schema.SchemaElements[4].Type)
	assert.Equal(t, "LIST", schema.SchemaElements[4].ConvertedType.String())

	assert.Equal(t, "REPEATED", schema.SchemaElements[5].RepetitionType.String())
	assert.Equal(t, int32(1), schema.SchemaElements[5].GetNumChildren())
	assert.Nil(t, schema.SchemaElements[5].Type)
	assert.Nil(t, schema.SchemaElements[5].ConvertedType)

	assert.Equal(t, "OPTIONAL", schema.SchemaElements[6].RepetitionType.String())
	assert.Equal(t, "BYTE_ARRAY", schema.SchemaElements[6].Type.String())
	assert.Equal(t, "UTF8", schema.SchemaElements[6].ConvertedType.String())
}

func Test_NewSchemaHandlerFromStruct_list_good_repeated(t *testing.T) {
	schema, err := NewSchemaHandlerFromStruct(new(struct {
		ListField []int32 `parquet:"name=repeated, type=INT32, repetitiontype=REPEATED"`
	}))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(schema.SchemaElements))
	assert.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
	assert.Equal(t, "ListField", schema.SchemaElements[1].Name)

	assert.Equal(t, "REPEATED", schema.SchemaElements[1].RepetitionType.String())
	assert.Equal(t, "INT32", schema.SchemaElements[1].Type.String())
	assert.Nil(t, schema.SchemaElements[1].ConvertedType)
}

func Test_NewSchemaHandlerFromSchemaHandler(t *testing.T) {
	schema1, err := NewSchemaHandlerFromStruct(new(struct {
		MapField1 map[string]*int32  `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
		MapField2 *map[*string]int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	}))
	assert.Nil(t, err)

	schema2 := NewSchemaHandlerFromSchemaHandler(schema1)
	assert.Equal(t, 9, len(schema2.SchemaElements))
	assert.Equal(t, "Parquet_go_root", schema2.SchemaElements[0].Name)
	assert.Equal(t, "MapField1", schema2.SchemaElements[1].Name)
	assert.Equal(t, "Key_value", schema2.SchemaElements[2].Name)
	assert.Equal(t, "Key", schema2.SchemaElements[3].Name)
	assert.Equal(t, "Value", schema2.SchemaElements[4].Name)
	assert.Equal(t, "MapField2", schema2.SchemaElements[5].Name)
	assert.Equal(t, "Key_value", schema2.SchemaElements[6].Name)
	assert.Equal(t, "Key", schema2.SchemaElements[7].Name)
	assert.Equal(t, "Value", schema2.SchemaElements[8].Name)

	assert.Equal(t, "REQUIRED", schema2.SchemaElements[1].RepetitionType.String())
	assert.Equal(t, int32(1), schema2.SchemaElements[1].GetNumChildren())
	assert.Nil(t, schema2.SchemaElements[1].Type)
	assert.Equal(t, "MAP", schema2.SchemaElements[1].ConvertedType.String())

	assert.Equal(t, "REPEATED", schema2.SchemaElements[2].RepetitionType.String())
	assert.Equal(t, int32(2), schema2.SchemaElements[2].GetNumChildren())
	assert.Nil(t, schema2.SchemaElements[2].Type)
	assert.Equal(t, "MAP_KEY_VALUE", schema2.SchemaElements[2].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema2.SchemaElements[3].RepetitionType.String())
	assert.Equal(t, "BYTE_ARRAY", schema2.SchemaElements[3].Type.String())
	assert.Equal(t, "UTF8", schema2.SchemaElements[3].ConvertedType.String())

	assert.Equal(t, "OPTIONAL", schema2.SchemaElements[4].RepetitionType.String())
	assert.Equal(t, "INT32", schema2.SchemaElements[4].Type.String())
	assert.Nil(t, schema2.SchemaElements[4].ConvertedType)

	assert.Equal(t, "OPTIONAL", schema2.SchemaElements[5].RepetitionType.String())
	assert.Equal(t, int32(1), schema2.SchemaElements[5].GetNumChildren())
	assert.Nil(t, schema2.SchemaElements[5].Type)
	assert.Equal(t, "MAP", schema2.SchemaElements[5].ConvertedType.String())

	assert.Equal(t, "REPEATED", schema2.SchemaElements[6].RepetitionType.String())
	assert.Equal(t, int32(2), schema2.SchemaElements[6].GetNumChildren())
	assert.Nil(t, schema2.SchemaElements[6].Type)
	assert.Equal(t, "MAP_KEY_VALUE", schema2.SchemaElements[6].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema2.SchemaElements[7].RepetitionType.String())
	assert.Equal(t, "BYTE_ARRAY", schema2.SchemaElements[7].Type.String())
	assert.Equal(t, "UTF8", schema2.SchemaElements[7].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema2.SchemaElements[8].RepetitionType.String())
	assert.Equal(t, "INT32", schema2.SchemaElements[8].Type.String())
	assert.Nil(t, schema2.SchemaElements[8].ConvertedType)
}

func Test_NewSchemaHandlerFromSchemaList(t *testing.T) {
	schema1, err := NewSchemaHandlerFromStruct(new(struct {
		MapField1 map[string]*int32  `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
		MapField2 *map[*string]int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	}))
	assert.Nil(t, err)

	schema2 := NewSchemaHandlerFromSchemaList(schema1.SchemaElements)
	assert.Equal(t, 9, len(schema2.SchemaElements))
	assert.Equal(t, "Parquet_go_root", schema2.SchemaElements[0].Name)
	assert.Equal(t, "MapField1", schema2.SchemaElements[1].Name)
	assert.Equal(t, "Key_value", schema2.SchemaElements[2].Name)
	assert.Equal(t, "Key", schema2.SchemaElements[3].Name)
	assert.Equal(t, "Value", schema2.SchemaElements[4].Name)
	assert.Equal(t, "MapField2", schema2.SchemaElements[5].Name)
	assert.Equal(t, "Key_value", schema2.SchemaElements[6].Name)
	assert.Equal(t, "Key", schema2.SchemaElements[7].Name)
	assert.Equal(t, "Value", schema2.SchemaElements[8].Name)

	assert.Equal(t, "REQUIRED", schema2.SchemaElements[1].RepetitionType.String())
	assert.Equal(t, int32(1), schema2.SchemaElements[1].GetNumChildren())
	assert.Nil(t, schema2.SchemaElements[1].Type)
	assert.Equal(t, "MAP", schema2.SchemaElements[1].ConvertedType.String())

	assert.Equal(t, "REPEATED", schema2.SchemaElements[2].RepetitionType.String())
	assert.Equal(t, int32(2), schema2.SchemaElements[2].GetNumChildren())
	assert.Nil(t, schema2.SchemaElements[2].Type)
	assert.Equal(t, "MAP_KEY_VALUE", schema2.SchemaElements[2].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema2.SchemaElements[3].RepetitionType.String())
	assert.Equal(t, "BYTE_ARRAY", schema2.SchemaElements[3].Type.String())
	assert.Equal(t, "UTF8", schema2.SchemaElements[3].ConvertedType.String())

	assert.Equal(t, "OPTIONAL", schema2.SchemaElements[4].RepetitionType.String())
	assert.Equal(t, "INT32", schema2.SchemaElements[4].Type.String())
	assert.Nil(t, schema2.SchemaElements[4].ConvertedType)

	assert.Equal(t, "OPTIONAL", schema2.SchemaElements[5].RepetitionType.String())
	assert.Equal(t, int32(1), schema2.SchemaElements[5].GetNumChildren())
	assert.Nil(t, schema2.SchemaElements[5].Type)
	assert.Equal(t, "MAP", schema2.SchemaElements[5].ConvertedType.String())

	assert.Equal(t, "REPEATED", schema2.SchemaElements[6].RepetitionType.String())
	assert.Equal(t, int32(2), schema2.SchemaElements[6].GetNumChildren())
	assert.Nil(t, schema2.SchemaElements[6].Type)
	assert.Equal(t, "MAP_KEY_VALUE", schema2.SchemaElements[6].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema2.SchemaElements[7].RepetitionType.String())
	assert.Equal(t, "BYTE_ARRAY", schema2.SchemaElements[7].Type.String())
	assert.Equal(t, "UTF8", schema2.SchemaElements[7].ConvertedType.String())

	assert.Equal(t, "REQUIRED", schema2.SchemaElements[8].RepetitionType.String())
	assert.Equal(t, "INT32", schema2.SchemaElements[8].Type.String())
	assert.Nil(t, schema2.SchemaElements[8].ConvertedType)
}
