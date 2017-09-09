package parquet_go

import (
	"errors"
	"parquet"
	"reflect"
	"strconv"
)

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

		schema := parquet.NewSchemaElement()
		schema.Name = item.Info["Name"]
		schema.RepetitionType = &(item.Info["RepetitionType"].(parquet.FieldRepetitionType))

		if item.GoType.Kind() == reflect.Struct {
			numField := item.GoType.NumField
			schema.NumChildren = &numField
			schema.Type = nil
			for i := 0; int32(i) < numField; i++ {
				f := item.GoType.Field(i)
				newItem := new(Item)
				newItem.GoType = f.Type
				newItem.Info["Name"] = f.Name

			}

		} else if item.GoType.Kind() == reflect.Slice {
		} else if item.GoType.Kind() == reflect.Map {
		} else {
		}

	}
}
