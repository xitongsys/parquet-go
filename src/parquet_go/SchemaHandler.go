package parquet_go

import (
	"errors"
	"parquet"
	"reflect"
	"strconv"
)

//path is full path

type SchemaHandler struct {
	SchemaElements []*parquet.SchemaElements
	MapIndex       map[string]int
}

func (self *SchemaHandler) GetRepetitionType(path []string) (parquet.FieldRepetitionType, error) {
	pathStr := PathToStr(path)
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
		pathStr := PathToStr(path[:i])
		rt, err := self.GetRepetitionType(pathStr)
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
		pathStr := PathToStr(path[:i])
		rt, err := self.GetRepetitionType(pathStr)
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
		return 0, error
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

type Item struct {
	GoType reflect.Type
	Info   map[string]interface{}
}

func NewSchemaHandlerFromStruct(obj interface{}) *SchemaHandler {
	ot := reflect.TypeOf(obj).Elem()
	item := new(Item)
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
			schema.RepetitionType = &(item.Info["RepetitionType"].(parquet.FieldRepetitionType))
			numField := item.GoType.NumField
			schema.NumChildren = &numField
			schema.Type = nil
			schemaElements = append(schemaElements, schema)

			for i := 0; int32(i) < numField; i++ {
				f := item.GoType.Field(i)
				newItem := new(Item)
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
			schema.ConvertedType = parquet.ConvertedType_LIST
			schemaElements = append(schemaElements, schema)

			schema = parquet.NewSchemaElement()
			schema.Name = "list"
			rt = parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt
			schema.Type = nil
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)

			newItem := new(Item)
			newItem.Info["Name"] = "element"
			rt = parquet.FieldRepetitionType_REQUIRED
			newItem.Info["RepetitionType"] = &rt
			newItem.Info["Tag"] = f.Tag
			stack = append(stack, newItem)

		} else if item.GoType.Kind() == reflect.Map {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info["Name"].(string)
			rt := item.Info["RepetitionType"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt
			var numField int32 = 1
			schema.NumChildren = &numField
			schema.Type = nil
			schema.ConvertedType = parquet.ConvertedType_MAP
			schemaElements = append(schemaElements, schema)

			schema := parquet.NewSchemaElement()
			schema.Name = "key_value"
			rt = parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt
			var numField int32 = 2
			schema.NumChildren = &numField
			schema.Type = nil
			schema.ConvertedType = parquet.ConvertedType_MAP_KEY_VALUE
			schemaElements = append(schemaElements, schema)

			newItem := new(Item)
			newItem.Info["Name"] = "key"
			newItem.GoType = item.GoType.Key()
			newItem.Info["RepetitionType"] = parquet.FieldRepetitionType_REQUIRED
			stack = append(stack, newItem)

			newItem = new(Item)
			newItem.Info["Name"] = "value"
			newItem.GoType = item.GoType.Elem()
			newItem.Info["RepetitionType"] = parquet.FieldRepetitionType_REQUIRED
			stack = append(stack, newItem)
		} else {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info["Name"]
			rt := item.Info["RepetitionType"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt
			schema.NumChildren = nil

			name := item.GoType.Name()
			if IsBaseType(name) {
				t := NameToBaseType(name)
				schema.Type = &t
			} else {
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
					ln := 12
					schema.Type = &t
					schema.ConvertedType = &ct
					schema.TypeLength = &ln
				} else if name == "DECIMAL" {
					tag := item.Info["Tag"].(reflect.StructTag)
					ct := NameToBaseType(name)
					bT := tag.Get("BaseType")
					t := NameToBaseType(bT)
					scale := int32(Atoi(tag.Get("Scale")))
					precision := int32(Atoi(tag.Get("Precision")))

					schema.Type = &t
					schema.ConvertedType = &ct
					schema.Scale = &scale
					schema.Precision = &precision

					if bt == "FIX_LEN_BYTE_ARRAY" {
						ln := int32(Atoi(tag.Get("Length")))
						schema.Length = &ln
					}
				}
			}

		}

	}
}
