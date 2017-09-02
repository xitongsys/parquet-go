package parquet_go

import (
	"errors"
	"log"
	"parquet"
	"reflect"
)

//////in schema, the path is the full path (including the root)////////

type SchemaHandler struct {
	SchemaMap map[string]*parquet.SchemaElement
	SchemaList []*parquet.SchemaElement
	RootName  string
}


type SchemaItem struct {
	GoType         reflect.Type
	Name           string
	RepetitionType parquet.FieldRepetitionType
}

func NewSchemaHandlerFromStruct(obj interface{}) *SchemaHandler {
	stack := make([]*SchemaItem, 0)

	ot := reflect.TypeOf(obj).Elem()
	item := new(SchemaItem)
	item.Name = "parquet_go_root"
	item.GoType = ot
	item.RepetitionType = parquet.FieldRepetitionType(-1)

	stack = append(stack, item)
	schemaList := make([]*parquet.SchemaElement, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item = stack[ln-1]
		stack = stack[:ln-1]

		schema := parquet.NewSchemaElement()
		schema.Name = item.Name
		schema.RepetitionType = &item.RepetitionType

		if item.GoType.Kind() == reflect.Struct {
			numField := TypeNumberField(item.GoType)
			schema.NumChildren = &numField
			schema.Type = nil

			for i := 0; int32(i) < numField; i++ {
				f := item.GoType.Field(i)
				newItem := new(SchemaItem)
				newItem.Name = f.Name
				//newItem.RepetitionType = TagToRepetitionType(f.Tag.Get("RepetitionType"))
				newItem.RepetitionType = parquet.FieldRepetitionType_REQUIRED
				newItem.GoType = f.Type
				stack = append(stack, newItem)
			}

		} else if item.GoType.Kind() == reflect.Slice {
			var numField int32 = 1
			schema.NumChildren = &numField
			schema.Type = nil

			et := item.GoType.Elem()
			newItem := new(SchemaItem)
			newItem.Name = item.Name + "_item"
			newItem.GoType = et
			newItem.RepetitionType = parquet.FieldRepetitionType_REPEATED
			stack = append(stack, newItem)
		} else {
			schema.NumChildren = nil
			parquetType := GoTypeToParquetType(item.GoType)
			schema.Type = &parquetType
			if parquetType == parquet.Type_BYTE_ARRAY {
				convertedType := parquet.ConvertedType_UTF8
				schema.ConvertedType = & convertedType
			}
		}

		schemaList = append(schemaList, schema)
	}

	log.Println(schemaList)

	return NewSchemaHandlerFromSchema(schemaList)
}

func NewSchemaHandlerFromSchema(schema []*parquet.SchemaElement) *SchemaHandler {
	schemaHandler := new(SchemaHandler)
	schemaHandler.SchemaMap = make(map[string]*parquet.SchemaElement)
	schemaHandler.SchemaList = schema

	//use DFS to get the path of the schema
	ln := len(schema)
	pos := 0
	stack := make([][]int32, 0)
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 {
			pair := make([]int32, 2)
			pair[0] = int32(pos)
			pair[1] = int32(*schema[pos].NumChildren)
			stack = append(stack, pair)
			pos++
		} else {
			top := stack[len(stack)-1]
			if top[1] == 0 {
				path := make([]string, 0)
				for i := 0; i < len(stack); i++ {
					path = append(path, schema[stack[i][0]].GetName())
				}
				schemaHandler.SchemaMap[PathToStr(path)] = schema[top[0]]
				stack = stack[:len(stack)-1]
			} else {
				top[1]--
				pair := make([]int32, 2)
				pair[0] = int32(pos)
				pair[1] = schema[pos].GetNumChildren()
				stack = append(stack, pair)
				pos++
			}
		}
	}
	if ln > 0 {
		schemaHandler.RootName = schema[0].GetName()
	}
	return schemaHandler
}

func (schemaHandler *SchemaHandler) GetType(pathStr string) (parquet.FieldRepetitionType, error) {
	if _, ok := schemaHandler.SchemaMap[pathStr]; ok {
		return schemaHandler.SchemaMap[pathStr].GetRepetitionType(), nil
	} else {
		return -1, errors.New("Name Not In Schema")
	}
}

func (schemaHandler *SchemaHandler) MaxDefinitionLevel(path []string) (int32, error) {
	var res int32 = 0
	ln := len(path)
	for i := 2; i <= ln; i++ {
		pathStr := PathToStr(path[:i])
		rType, err := schemaHandler.GetType(pathStr)
		if err != nil {
			return 0, err
		}
		if rType != parquet.FieldRepetitionType_REQUIRED {
			res++
		}
	}
	return res, nil
}

func (schemaHandler *SchemaHandler) MaxRepetitionLevel(path []string) (int32, error) {
	var res int32 = 0
	ln := len(path)
	for i := 2; i <= ln; i++ {
		pathStr := PathToStr(path[:i])
		rType, err := schemaHandler.GetType(pathStr)
		if err != nil {
			return 0, err
		}
		if rType == parquet.FieldRepetitionType_REPEATED {
			res++
		}
	}
	return res, nil
}

func (schemaHandler *SchemaHandler) IndexFromRepetitionLevel(path []string, repetitionLevel int32) int32 {
	ln := int32(len(path))
	var cur int32 = -1
	var i int32 = 1
	for cur < repetitionLevel && i < ln {
		t, _ := schemaHandler.GetType(PathToStr(path[:i+1]))
		if t == parquet.FieldRepetitionType_REPEATED {
			cur++
		}
		i++
	}
	return (i - 1)
}

func (schemaHandler *SchemaHandler) IndexFromDefinitionLevel(path []string, definitionLevel int32) int32 {
	var ln int32 = int32(len(path))
	var cur int32 = -1
	var i int32 = 1
	for cur < definitionLevel && i < int32(ln) {
		t, _ := schemaHandler.GetType(PathToStr(path[:i+1]))
		if t != parquet.FieldRepetitionType_REQUIRED {
			cur++
		}
		i++
	}
	return (i - 1)
}
