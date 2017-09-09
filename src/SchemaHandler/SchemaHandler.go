package SchemaHandler

import (
	"Common"
	"ParquetType"
	"errors"
	"log"
	"parquet"
	"reflect"
	"strconv"
)

//path is full path

type SchemaHandler struct {
	SchemaElements []*parquet.SchemaElement
	MapIndex       map[string]int32
}

func (self *SchemaHandler) GetRepetitionType(path []string) (parquet.FieldRepetitionType, error) {
	pathStr := Common.PathToStr(path)
	if index, ok := self.MapIndex[pathStr]; ok {
		return self.SchemaElements[index].GetRepetitionType(), nil
	} else {
		return 0, errors.New("Name Not In Schema")
	}
}

func (self *SchemaHandler) MaxDefinitionLevel(path []string) (int32, error) {
	var res int32 = 0
	ln := len(path)
	for i := 2; i <= ln; i++ {
		rt, err := self.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}
		if rt != parquet.FieldRepetitionType_REQUIRED {
			res++
		}
	}
	return res, nil
}

func (self *SchemaHandler) MaxRepetitionLevel(path []string) (int32, error) {
	var res int32 = 0
	ln := len(path)
	for i := 2; i <= ln; i++ {
		rt, err := self.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}
		if rt == parquet.FieldRepetitionType_REPEATED {
			res++
		}
	}
	return res, nil
}

func (self *SchemaHandler) IndexFromRepetitionLevel(path []string, rl int32) (int32, error) {
	if rl <= 0 {
		return 0, nil
	}
	ln := len(path)
	i := 0
	var cur int32 = 0
	for cur < rl && i+1 < ln {
		i++
		t, err := self.GetRepetitionType(path[:i+1])
		if err != nil {
			return 0, err
		}
		if t == parquet.FieldRepetitionType_REPEATED {
			cur++
		}
	}
	return int32(i), nil
}

func (self *SchemaHandler) IndexFromDefinitionLevel(path []string, dl int32) (int32, error) {
	if dl <= 0 {
		return 0, nil
	}
	ln := len(path)
	i := 0
	var cur int32 = 0
	for cur < dl && i+1 < ln {
		i++
		t, err := self.GetRepetitionType(path[:i+1])
		if err != nil {
			return 0, err
		}
		if t == parquet.FieldRepetitionType_REPEATED {
			cur++
		}
	}
	return int32(i), nil
}

func (self *SchemaHandler) GetRootName() string {
	if len(self.SchemaElements) <= 0 {
		return ""
	}
	return self.SchemaElements[0].GetName()
}

type Item struct {
	GoType reflect.Type
	Info   map[string]interface{}
}

func NewItem() *Item {
	item := new(Item)
	item.Info = make(map[string]interface{})
	return item
}

func NewSchemaHandlerFromStruct(obj interface{}) *SchemaHandler {
	ot := reflect.TypeOf(obj).Elem()
	item := NewItem()
	item.GoType = ot
	item.Info["Name"] = "parquet_go_root"
	item.Info["RepetitionType"] = parquet.FieldRepetitionType(-1)

	stack := make([]*Item, 0)
	stack = append(stack, item)
	schemaElements := make([]*parquet.SchemaElement, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item = stack[ln-1]
		stack = stack[:ln-1]

		if item.GoType.Kind() == reflect.Struct {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info["Name"].(string)
			rt := item.Info["RepetitionType"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt
			numField := int32(item.GoType.NumField())
			schema.NumChildren = &numField
			schema.Type = nil
			schemaElements = append(schemaElements, schema)

			for i := 0; int32(i) < numField; i++ {
				f := item.GoType.Field(i)
				newItem := NewItem()
				newItem.Info["Name"] = f.Name
				newItem.Info["Tag"] = f.Tag
				if f.Type.Kind() == reflect.Ptr {
					newItem.GoType = f.Type.Elem()
					newItem.Info["RepetitionType"] = parquet.FieldRepetitionType_OPTIONAL
				} else {
					newItem.GoType = f.Type
					newItem.Info["RepetitionType"] = parquet.FieldRepetitionType_REQUIRED
				}
				stack = append(stack, newItem)
			}
		} else if item.GoType.Kind() == reflect.Slice {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info["Name"].(string)
			rt := item.Info["RepetitionType"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt
			var numField int32 = 1
			schema.NumChildren = &numField
			schema.Type = nil
			ct := parquet.ConvertedType_LIST
			schema.ConvertedType = &ct
			schemaElements = append(schemaElements, schema)

			schema = parquet.NewSchemaElement()
			schema.Name = "list"
			rt = parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt
			schema.Type = nil
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)

			newItem := NewItem()
			newItem.Info["Name"] = "element"
			newItem.GoType = item.GoType.Elem()
			newItem.Info["RepetitionType"] = parquet.FieldRepetitionType_REQUIRED
			newItem.Info["Tag"] = item.Info["Tag"]
			stack = append(stack, newItem)

		} else if item.GoType.Kind() == reflect.Map {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info["Name"].(string)
			rt := item.Info["RepetitionType"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt
			var numField int32 = 1
			schema.NumChildren = &numField
			schema.Type = nil
			ct := parquet.ConvertedType_MAP
			schema.ConvertedType = &ct
			schemaElements = append(schemaElements, schema)

			schema = parquet.NewSchemaElement()
			schema.Name = "key_value"
			rt = parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt
			numField = 2
			schema.NumChildren = &numField
			schema.Type = nil
			ct = parquet.ConvertedType_MAP_KEY_VALUE
			schema.ConvertedType = &ct
			schemaElements = append(schemaElements, schema)

			newItem := NewItem()
			newItem.Info["Name"] = "key"
			newItem.GoType = item.GoType.Key()
			newItem.Info["RepetitionType"] = parquet.FieldRepetitionType_REQUIRED
			stack = append(stack, newItem)

			newItem = NewItem()
			newItem.Info["Name"] = "value"
			newItem.GoType = item.GoType.Elem()
			newItem.Info["RepetitionType"] = parquet.FieldRepetitionType_REQUIRED
			stack = append(stack, newItem)
		} else {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info["Name"].(string)
			rt := item.Info["RepetitionType"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt
			schema.NumChildren = nil

			name := item.GoType.Name()
			if ParquetType.IsBaseType(name) {
				t := ParquetType.NameToBaseType(name)
				schema.Type = &t
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
					tag := item.Info["Tag"].(reflect.StructTag)
					ct := ParquetType.NameToConvertedType(name)
					bT := tag.Get("BaseType")
					t := ParquetType.NameToBaseType(bT)
					scaleTmp, _ := strconv.Atoi(tag.Get("Scale"))
					precisionTmp, _ := strconv.Atoi(tag.Get("Precision"))
					scale := int32(scaleTmp)
					precision := int32(precisionTmp)

					schema.Type = &t
					schema.ConvertedType = &ct
					schema.Scale = &scale
					schema.Precision = &precision

					if bT == "FIX_LEN_BYTE_ARRAY" {
						lnTmp, _ := strconv.Atoi(tag.Get("Length"))
						ln := int32(lnTmp)
						schema.TypeLength = &ln
					}
				}
			}
			schemaElements = append(schemaElements, schema)
		}
	}

	log.Println(schemaElements)
	return NewSchemaHandlerFromSchemaList(schemaElements)
}

func NewSchemaHandlerFromSchemaList(schemas []*parquet.SchemaElement) *SchemaHandler {
	schemaHandler := new(SchemaHandler)
	schemaHandler.MapIndex = make(map[string]int32)
	schemaHandler.SchemaElements = schemas

	//use DFS get path of schema
	ln := int32(len(schemas))
	var pos int32 = 0
	stack := make([][]int32, 0) //stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 {
			item := make([]int32, 2)
			item[0] = pos
			item[1] = int32(*schemas[pos].NumChildren)
			stack = append(stack, item)
			pos++
		} else {
			top := stack[len(stack)-1]
			if top[1] == 0 {
				path := make([]string, 0)
				for i := 0; i < len(stack); i++ {
					path = append(path, schemas[stack[i][0]].GetName())
				}
				schemaHandler.MapIndex[Common.PathToStr(path)] = top[0]
				stack = stack[:len(stack)-1]
			} else {
				top[1]--
				item := make([]int32, 2)
				item[0] = pos
				item[1] = schemas[pos].GetNumChildren()
				stack = append(stack, item)
				pos++
			}
		}
	}
	return schemaHandler
}
