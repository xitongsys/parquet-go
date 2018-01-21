package SchemaHandler

import (
	"errors"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
)

//  PathMap Example
//              root(a dummy root)  (Path: "root", Children: A)
//               |
//               A  (Path:"root/A", Childend: B,C)
//          /           \
//  B(Path:"root/A/B")   C(Path:"root/A/C")

// PathMapType records the path and its children; This is used in Marshal for improve performance.
type PathMapType struct {
	Path     string
	Children map[string]*PathMapType
}

// NewPathMap return a PathMapType
func NewPathMap(path string) *PathMapType {
	return &PathMapType{
		Path:     path,
		Children: make(map[string]*PathMapType),
	}
}

// Add builds PathTree when reach a leaf node
func (self *PathMapType) Add(path []string) {
	ln := len(path)
	if ln <= 1 {
		return // len == 1 means reaching the leaf node
	}
	c := path[1]
	if _, ok := self.Children[c]; !ok {
		self.Children[c] = NewPathMap(self.Path + "." + c)
	}
	self.Children[c].Add(path[1:])
}

// SchemaHandler stores the schema data
type SchemaHandler struct {
	SchemaElements []*parquet.SchemaElement
	MapIndex       map[string]int32
	IndexMap       map[int32]string
	PathMap        *PathMapType
	Infos          []map[string]interface{}

	InPathToExPath map[string]string
	ExPathToInPath map[string]string

	ValueColumns []string // all leaf node
}

// SetValueColumns collects leaf nodes' full path in SchemaHandler.ValueColumns
func (self *SchemaHandler) SetValueColumns() {
	for i := 0; i < len(self.SchemaElements); i++ {
		schema := self.SchemaElements[i]
		if schema.GetNumChildren() == 0 {
			pathStr := self.IndexMap[int32(i)]
			self.ValueColumns = append(self.ValueColumns, pathStr)
		}
	}
}

func (self *SchemaHandler) GetColumnNum() int64 {
	return int64(len(self.ValueColumns))
}

// SetPathMap builds the PathMap from leaf SchemaElement
func (self *SchemaHandler) SetPathMap() {
	self.PathMap = NewPathMap(self.GetRootName())
	for i := 0; i < len(self.SchemaElements); i++ {
		schema := self.SchemaElements[i]
		if schema.GetNumChildren() == 0 {
			pathStr := self.IndexMap[int32(i)]
			self.PathMap.Add(Common.StrToPath(pathStr))
		}
	}
}

// GetRepetitionType returns the repetition type of a column by it's schema path
func (self *SchemaHandler) GetRepetitionType(path []string) (parquet.FieldRepetitionType, error) {
	pathStr := Common.PathToStr(path)
	if index, ok := self.MapIndex[pathStr]; ok {
		return self.SchemaElements[index].GetRepetitionType(), nil
	} else {
		return 0, errors.New("Name Not In Schema")
	}
}

// MaxDefinitionLevel returns the max definition level type of a column by it's schema path
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

// MaxRepetitionLevel returns the max repetition level type of a column by it's schema path
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

// IndexFromRepetitionLevel returns the index from the repetition level
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

// IndexFromDefinitionLevel returns the index from the definition level, reuse IndexFromRepetitionLevel
func (self *SchemaHandler) IndexFromDefinitionLevel(path []string, dl int32) (int32, error) {
	return self.IndexFromRepetitionLevel(path, dl)
}

func (self *SchemaHandler) CreateInExMap() {
	//use DFS get path of schema
	schemas := self.SchemaElements
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
				inPath, exPath := make([]string, 0), make([]string, 0)
				for i := 0; i < len(stack); i++ {
					inPath = append(inPath, self.Infos[stack[i][0]]["inname"].(string))
					exPath = append(exPath, self.Infos[stack[i][0]]["exname"].(string))
				}
				inPathStr, exPathStr := Common.PathToStr(inPath), Common.PathToStr(exPath)
				self.ExPathToInPath[exPathStr] = inPathStr
				self.InPathToExPath[inPathStr] = exPathStr

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
}

//Get root name from the schema handler
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

//Create schema handler from a object
func NewSchemaHandlerFromStruct(obj interface{}) (sh *SchemaHandler, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	ot := reflect.TypeOf(obj).Elem()
	item := NewItem()
	item.GoType = ot
	item.Info["inname"] = "parquet_go_root"
	item.Info["exname"] = "parquet_go_root"
	item.Info["repetitiontype"] = parquet.FieldRepetitionType(-1)

	stack := make([]*Item, 1)
	stack[0] = item
	schemaElements := make([]*parquet.SchemaElement, 0)
	infos := make([]map[string]interface{}, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item = stack[ln-1]
		stack = stack[:ln-1]

		if item.GoType.Kind() == reflect.Struct {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info["inname"].(string)
			rt := item.Info["repetitiontype"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt
			numField := int32(item.GoType.NumField())
			schema.NumChildren = &numField
			schema.Type = nil
			schemaElements = append(schemaElements, schema)

			info := Common.NewTagMapFromCopy(item.Info)
			infos = append(infos, info)

			for i := int(numField - 1); i >= 0; i-- {
				f := item.GoType.Field(i)
				newItem := NewItem()
				newItem.Info = Common.TagToMap(f.Tag.Get("parquet"))
				newItem.Info["inname"] = f.Name
				newItem.GoType = f.Type
				if f.Type.Kind() == reflect.Ptr {
					newItem.GoType = f.Type.Elem()
					newItem.Info["repetitiontype"] = parquet.FieldRepetitionType_OPTIONAL
				}
				stack = append(stack, newItem)
			}
		} else if item.GoType.Kind() == reflect.Slice &&
			item.Info["repetitiontype"].(parquet.FieldRepetitionType) != parquet.FieldRepetitionType_REPEATED {

			schema := parquet.NewSchemaElement()
			schema.Name = item.Info["inname"].(string)
			rt1 := item.Info["repetitiontype"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			schema.Type = nil
			ct1 := parquet.ConvertedType_LIST
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			info := Common.NewTagMapFromCopy(item.Info)
			infos = append(infos, info)

			schema = parquet.NewSchemaElement()
			schema.Name = "list"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			schema.Type = nil
			var numField2 int32 = 1
			schema.NumChildren = &numField2
			schemaElements = append(schemaElements, schema)
			info = Common.NewTagMapFromCopy(item.Info)
			info["inname"], info["exname"] = "list", "list"
			infos = append(infos, info)

			newItem := NewItem()
			newItem.Info = Common.GetValueTagMap(item.Info)
			newItem.Info["inname"] = "element"
			newItem.Info["exname"] = "element"
			newItem.GoType = item.GoType.Elem()
			if newItem.GoType.Kind() == reflect.Ptr {
				newItem.Info["repetitiontype"] = parquet.FieldRepetitionType_OPTIONAL
				newItem.GoType = item.GoType.Elem().Elem()
			} else {
				newItem.Info["repetitiontype"] = parquet.FieldRepetitionType_REQUIRED
			}
			stack = append(stack, newItem)

		} else if item.GoType.Kind() == reflect.Slice &&
			item.Info["repetitiontype"].(parquet.FieldRepetitionType) == parquet.FieldRepetitionType_REPEATED {
			newItem := NewItem()
			newItem.Info = item.Info
			newItem.GoType = item.GoType.Elem()
			stack = append(stack, newItem)

		} else if item.GoType.Kind() == reflect.Map {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info["inname"].(string)
			rt1 := item.Info["repetitiontype"].(parquet.FieldRepetitionType)
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			schema.Type = nil
			ct1 := parquet.ConvertedType_MAP
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			info := Common.NewTagMapFromCopy(item.Info)
			infos = append(infos, info)

			schema = parquet.NewSchemaElement()
			schema.Name = "key_value"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			var numField2 int32 = 2
			schema.NumChildren = &numField2
			schema.Type = nil
			ct2 := parquet.ConvertedType_MAP_KEY_VALUE
			schema.ConvertedType = &ct2
			schemaElements = append(schemaElements, schema)
			info = Common.NewTagMapFromCopy(item.Info)
			info["inname"], info["exname"] = "key_value", "key_value"
			infos = append(infos, info)

			newItem := NewItem()
			newItem.Info = Common.GetValueTagMap(item.Info)
			newItem.GoType = item.GoType.Elem()
			if newItem.GoType.Kind() == reflect.Ptr {
				newItem.Info["repetitiontype"] = parquet.FieldRepetitionType_OPTIONAL
				newItem.GoType = item.GoType.Elem().Elem()
			} else {
				newItem.Info["repetitiontype"] = parquet.FieldRepetitionType_REQUIRED
			}
			stack = append(stack, newItem)

			newItem = NewItem()
			newItem.Info = Common.GetKeyTagMap(item.Info)
			newItem.GoType = item.GoType.Key()
			newItem.Info["repetitiontype"] = parquet.FieldRepetitionType_REQUIRED
			stack = append(stack, newItem)

		} else {
			schema := Common.NewSchemaElementFromTagMap(item.Info)
			schemaElements = append(schemaElements, schema)
			info := Common.NewTagMapFromCopy(item.Info)
			infos = append(infos, info)
		}
	}
	res, err2 := NewSchemaHandlerFromSchemaList(schemaElements)
	res.Infos = infos
	res.CreateInExMap()
	return res, err2
}

//Create schema handler from schema list
func NewSchemaHandlerFromSchemaList(schemas []*parquet.SchemaElement) (sh *SchemaHandler, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	schemaHandler := new(SchemaHandler)
	schemaHandler.MapIndex = make(map[string]int32)
	schemaHandler.IndexMap = make(map[int32]string)
	schemaHandler.InPathToExPath = make(map[string]string)
	schemaHandler.ExPathToInPath = make(map[string]string)
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
				schemaHandler.IndexMap[top[0]] = Common.PathToStr(path)
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
	//	log.Println("NewSchemaHandlerFromSchemaList Finished")
	schemaHandler.SetPathMap()
	schemaHandler.SetValueColumns()
	return schemaHandler, nil
}
