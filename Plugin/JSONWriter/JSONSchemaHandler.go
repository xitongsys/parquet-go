package JSONWriter

import (
	"encoding/json"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

type JSONSchemaItemType struct {
	Tag    string
	Fields []*JSONSchemaItemType
}

func NewJSONSchemaItem() *JSONSchemaItemType {
	return new(JSONSchemaItemType)
}

func NewSchemaHandlerFromJSON(str string) *SchemaHandler.SchemaHandler {
	schema := NewJSONSchemaItem()
	json.Unmarshal([]byte(str), schema)

	stack := make([]*JSONSchemaItemType, 0)
	stack = append(stack, schema)
	schemaElements := make([]*parquet.SchemaElement, 0)
	infos := make([]map[string]interface{}, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item := stack[ln-1]
		stack = stack[:ln-1]
		info := Common.TagToMap(item.Tag)

		if info["type"].(string) == "" {
			schema := parquet.NewSchemaElement()
			schema.Name = info["inname"].(string)
			rt := info["repetitiontype"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt
			numField := int32(len(item.Fields))
			schema.NumChildren = &numField
			schema.Type = nil
			schemaElements = append(schemaElements, schema)

			newInfo := Common.NewTagMapFromCopy(info)
			infos = append(infos, newInfo)

			for i := int(numField - 1); i >= 0; i-- {
				newItem := item.Fields[i]
				stack = append(stack, newItem)
			}

		} else if info["type"].(string) == "LIST" {
			schema := parquet.NewSchemaElement()
			schema.Name = info["inname"].(string)
			rt1 := info["repetitiontype"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			schema.Type = nil
			ct1 := parquet.ConvertedType_LIST
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			info := Common.NewTagMapFromCopy(info)
			infos = append(infos, info)

			schema = parquet.NewSchemaElement()
			schema.Name = "list"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			schema.Type = nil
			var numField2 int32 = 1
			schema.NumChildren = &numField2
			schemaElements = append(schemaElements, schema)
			info = Common.NewTagMapFromCopy(info)
			info["inname"], info["exname"] = "list", "list"
			infos = append(infos, info)

			stack = append(stack, item.Fields[0])

		} else if info["type"].(string) == "MAP" {
			schema := parquet.NewSchemaElement()
			schema.Name = info["inname"].(string)
			rt1 := info["repetitiontype"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			schema.Type = nil
			ct1 := parquet.ConvertedType_MAP
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			info := Common.NewTagMapFromCopy(info)
			infos = append(infos, info)

			schema = parquet.NewSchemaElement()
			schema.Name = "key_value"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			ct2 := parquet.ConvertedType_MAP_KEY_VALUE
			schema.ConvertedType = &ct2
			schema.Type = nil
			var numField2 int32 = 2
			schema.NumChildren = &numField2
			schemaElements = append(schemaElements, schema)
			info = Common.NewTagMapFromCopy(info)
			info["inname"], info["exname"] = "key_value", "key_value"
			infos = append(infos, info)

			stack = append(stack, item.Fields[1]) //put value
			stack = append(stack, item.Fields[0]) //put key

		} else {
			schema := Common.NewSchemaElementFromTagMap(info)
			schemaElements = append(schemaElements, schema)
			info := Common.NewTagMapFromCopy(info)
			infos = append(infos, info)
		}
	}

	res := SchemaHandler.NewSchemaHandlerFromSchemaList(schemaElements)
	res.Infos = infos
	res.CreateInExMap()

	return res

}
