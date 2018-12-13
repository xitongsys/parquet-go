package SchemaHandler

import (
	"errors"
	"reflect"

	"github.com/pmalekn/parquet-go/Common"
	"github.com/pmalekn/parquet-go/parquet"
)

/*
PathMap Example
            root(a dummy root)  (Path: "root", Children: A)
             |
             A  (Path:"root/A", Childend: B,C)
        /           \
B(Path:"root/A/B")   C(Path:"root/A/C")
*/

// PathMapType records the path and its children; This is used in Marshal for improve performance.
type PathMapType struct {
	Path     string
	Children map[string]*PathMapType
}

func NewPathMap(path string) *PathMapType {
	pathMap := new(PathMapType)
	pathMap.Path = path
	pathMap.Children = make(map[string]*PathMapType)
	return pathMap
}

func (self *PathMapType) Add(path []string) {
	ln := len(path)
	if ln <= 1 {
		return
	}
	c := path[1]
	if _, ok := self.Children[c]; !ok {
		self.Children[c] = NewPathMap(self.Path + "." + c)
	}
	self.Children[c].Add(path[1:])
}

/////////////////pathMap///////////////////////////

// SchemaHandler stores the schema data
type SchemaHandler struct {
	SchemaElements []*parquet.SchemaElement
	MapIndex       map[string]int32
	IndexMap       map[int32]string
	PathMap        *PathMapType
	Infos          []*Common.Tag

	InPathToExPath map[string]string
	ExPathToInPath map[string]string

	ValueColumns []string
}

// setValueColumns collects leaf nodes' full path in SchemaHandler.ValueColumns
func (self *SchemaHandler) setValueColumns() {
	for i := 0; i < len(self.SchemaElements); i++ {
		schema := self.SchemaElements[i]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			pathStr := self.IndexMap[int32(i)]
			self.ValueColumns = append(self.ValueColumns, pathStr)
		}
	}
}

func (self *SchemaHandler) GetColumnNum() int64 {
	return int64(len(self.ValueColumns))
}

// setPathMap builds the PathMap from leaf SchemaElement
func (self *SchemaHandler) setPathMap() {
	self.PathMap = NewPathMap(self.GetRootName())
	for i := 0; i < len(self.SchemaElements); i++ {
		schema := self.SchemaElements[i]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
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
	}
	return 0, errors.New("Name Not In Schema")
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

func (self *SchemaHandler) CreateInExMap() {
	//use DFS get path of schema
	schemas := self.SchemaElements
	ln := int32(len(schemas))
	var pos int32 = 0
	stack := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}
			item := [2]int32{pos, schemas[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else { // leaf node
			inPath, exPath := make([]string, 0), make([]string, 0)
			for i := 0; i < len(stack); i++ {
				inPath = append(inPath, self.Infos[stack[i][0]].InName)
				exPath = append(exPath, self.Infos[stack[i][0]].ExName)
			}
			inPathStr, exPathStr := Common.PathToStr(inPath), Common.PathToStr(exPath)
			self.ExPathToInPath[exPathStr] = inPathStr
			self.InPathToExPath[inPathStr] = exPathStr
			stack = stack[:len(stack)-1]
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
	Info   *Common.Tag
}

func NewItem() *Item {
	item := new(Item)
	item.Info = Common.NewTag()
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
	item.Info.InName = "parquet_go_root"
	item.Info.ExName = "parquet_go_root"
	item.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED

	stack := make([]*Item, 1)
	stack[0] = item
	schemaElements := make([]*parquet.SchemaElement, 0)
	infos := make([]*Common.Tag, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item = stack[ln-1]
		stack = stack[:ln-1]
		var newInfo *Common.Tag

		if item.GoType.Kind() == reflect.Struct {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			schema.RepetitionType = &item.Info.RepetitionType
			numField := int32(item.GoType.NumField())
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)

			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)

			for i := int(numField - 1); i >= 0; i-- {
				f := item.GoType.Field(i)
				tagStr := f.Tag.Get("parquet")

				//ignore item without parquet tag
				if len(tagStr) <= 0 {
					numField--
					continue
				}

				newItem := NewItem()
				newItem.Info = Common.StringToTag(tagStr)
				newItem.Info.InName = f.Name
				newItem.GoType = f.Type
				if f.Type.Kind() == reflect.Ptr {
					newItem.GoType = f.Type.Elem()
					newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
				}
				stack = append(stack, newItem)
			}
		} else if item.GoType.Kind() == reflect.Slice &&
			item.Info.RepetitionType != parquet.FieldRepetitionType_REPEATED {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			rt1 := item.Info.RepetitionType
			schema.RepetitionType = &rt1
			var numField int32 = 1
			schema.NumChildren = &numField
			ct1 := parquet.ConvertedType_LIST
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "list"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			newInfo.InName, newInfo.ExName = "list", "list"
			infos = append(infos, newInfo)

			newItem := NewItem()
			newItem.Info = Common.GetValueTagMap(item.Info)
			newItem.Info.InName = "element"
			newItem.Info.ExName = "element"
			newItem.GoType = item.GoType.Elem()
			if newItem.GoType.Kind() == reflect.Ptr {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
				newItem.GoType = item.GoType.Elem().Elem()
			} else {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			}
			stack = append(stack, newItem)

		} else if item.GoType.Kind() == reflect.Slice &&
			item.Info.RepetitionType == parquet.FieldRepetitionType_REPEATED {
			newItem := NewItem()
			newItem.Info = item.Info
			newItem.GoType = item.GoType.Elem()
			stack = append(stack, newItem)

		} else if item.GoType.Kind() == reflect.Map {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			rt1 := item.Info.RepetitionType
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			ct1 := parquet.ConvertedType_MAP
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "key_value"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			var numField2 int32 = 2
			schema.NumChildren = &numField2
			ct2 := parquet.ConvertedType_MAP_KEY_VALUE
			schema.ConvertedType = &ct2
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			newInfo.InName, newInfo.ExName = "key_value", "key_value"
			infos = append(infos, newInfo)

			newItem := NewItem()
			newItem.Info = Common.GetValueTagMap(item.Info)
			newItem.GoType = item.GoType.Elem()
			if newItem.GoType.Kind() == reflect.Ptr {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
				newItem.GoType = item.GoType.Elem().Elem()
			} else {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			}
			stack = append(stack, newItem)

			newItem = NewItem()
			newItem.Info = Common.GetKeyTagMap(item.Info)
			newItem.GoType = item.GoType.Key()
			newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			stack = append(stack, newItem)

		} else {
			schema := Common.NewSchemaElementFromTagMap(item.Info)
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)
		}
	}

	res := NewSchemaHandlerFromSchemaList(schemaElements)
	res.Infos = infos
	res.CreateInExMap()
	return res, nil
}

// NewSchemaHandlerFromSchemaList creates schema handler from schema list
func NewSchemaHandlerFromSchemaList(schemas []*parquet.SchemaElement) *SchemaHandler {
	schemaHandler := new(SchemaHandler)
	schemaHandler.MapIndex = make(map[string]int32)
	schemaHandler.IndexMap = make(map[int32]string)
	schemaHandler.InPathToExPath = make(map[string]string)
	schemaHandler.ExPathToInPath = make(map[string]string)
	schemaHandler.SchemaElements = schemas

	//use DFS get path of schema
	ln := int32(len(schemas))
	var pos int32 = 0
	stack := make([][2]int32, 0) //stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}
			item := [2]int32{pos, schemas[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else {
			path := make([]string, 0)
			for i := 0; i < len(stack); i++ {
				path = append(path, schemas[stack[i][0]].GetName())
			}
			topPos := stack[len(stack)-1][0]
			schemaHandler.MapIndex[Common.PathToStr(path)] = topPos
			schemaHandler.IndexMap[topPos] = Common.PathToStr(path)
			stack = stack[:len(stack)-1]
		}
	}
	schemaHandler.setPathMap()
	schemaHandler.setValueColumns()
	return schemaHandler
}
