package Marshal

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

//ss is []string
func MarshalJSON(ss []interface{}, bgn int, end int, schemaHandler *SchemaHandler.SchemaHandler) (tb *map[string]*Layout.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	res := make(map[string]*Layout.Table)
	pathMap := schemaHandler.PathMap
	nodeBuf := NewNodeBuf(1)

	for i := 0; i < len(schemaHandler.SchemaElements); i++ {
		schema := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res[pathStr] = Layout.NewEmptyTable()
			res[pathStr].Path = Common.StrToPath(pathStr)
			res[pathStr].MaxDefinitionLevel, _ = schemaHandler.MaxDefinitionLevel(res[pathStr].Path)
			res[pathStr].MaxRepetitionLevel, _ = schemaHandler.MaxRepetitionLevel(res[pathStr].Path)
			res[pathStr].RepetitionType = schema.GetRepetitionType()
			res[pathStr].Type = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]].GetType()
			res[pathStr].Info = schemaHandler.Infos[i]
		}
	}

	stack := make([]*Node, 0, 100)
	for i := bgn; i < end; i++ {
		stack = stack[:0]
		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		var ui interface{}

		// `useNumber`causes the Decoder to unmarshal a number into an interface{} as a Number instead of as a float64.
		d := json.NewDecoder(strings.NewReader(ss[i].(string)))
		d.UseNumber()
		d.Decode(&ui)

		node.Val = reflect.ValueOf(ui)
		node.PathMap = pathMap

		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node = stack[ln-1]
			stack = stack[:ln-1]

			tk := node.Val.Type().Kind()

			pathStr := node.PathMap.Path

			schemaIndex, ok := schemaHandler.MapIndex[pathStr]
			//no schema item will be ignored
			if !ok {
				continue
			}

			info := schemaHandler.Infos[schemaIndex]

			if tk == reflect.Map {
				keys := node.Val.MapKeys()

				if info.Type == "MAP" { //real map
					pathStr = pathStr + ".key_value"
					if len(keys) <= 0 {
						for key, table := range res {
							if len(key) >= len(node.PathMap.Path) &&
								key[:len(node.PathMap.Path)] == node.PathMap.Path {
								table.Values = append(table.Values, nil)
								table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
								table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
							}
						}
					}

					rlNow, _ := schemaHandler.MaxRepetitionLevel(Common.StrToPath(pathStr))
					for j := len(keys) - 1; j >= 0; j-- {
						key := keys[j]
						value := node.Val.MapIndex(key).Elem()

						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children["key_value"].Children["key"]
						newNode.Val = key
						newNode.DL = node.DL + 1
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						stack = append(stack, newNode)

						newNode = nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children["key_value"].Children["value"]
						newNode.Val = value
						newNode.DL = node.DL + 1
						newPathStr := newNode.PathMap.Path // check again
						newSchemaIndex := schemaHandler.MapIndex[newPathStr]
						newInfo := schemaHandler.Infos[newSchemaIndex]
						if newInfo.RepetitionType == parquet.FieldRepetitionType_OPTIONAL { //map value only be :optional or required
							newNode.DL++
						}

						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						stack = append(stack, newNode)
					}

				} else { //struct
					keysMap := make(map[string]bool)
					for j := 0; j < len(keys); j++ {
						keysMap[keys[j].String()] = true
					}
					for key, _ := range node.PathMap.Children {
						_, ok := keysMap[key]
						if ok && node.Val.MapIndex(reflect.ValueOf(key)).Elem().IsValid() {

							newNode := nodeBuf.GetNode()
							newNode.PathMap = node.PathMap.Children[key]
							newNode.Val = node.Val.MapIndex(reflect.ValueOf(key)).Elem()
							newNode.RL = node.RL
							newNode.DL = node.DL
							newPathStr := newNode.PathMap.Path
							newSchemaIndex := schemaHandler.MapIndex[newPathStr]
							newInfo := schemaHandler.Infos[newSchemaIndex]
							if newInfo.RepetitionType == parquet.FieldRepetitionType_OPTIONAL {
								newNode.DL++
							}
							stack = append(stack, newNode)

						} else {
							newPathStr := node.PathMap.Children[key].Path
							for path, table := range res {
								if strings.HasPrefix(path, newPathStr) &&
									(len(path) == len(newPathStr) || path[len(newPathStr)] == '.') {

									table.Values = append(table.Values, nil)
									table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
									table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
								}
							}
						}
					}
				}

			} else if tk == reflect.Slice {
				ln := node.Val.Len()

				if info.Type == "LIST" { //real LIST
					pathStr = pathStr + ".list" + ".element"
					if ln <= 0 {
						for key, table := range res {
							if len(key) >= len(node.PathMap.Path) &&
								key[:len(node.PathMap.Path)] == node.PathMap.Path {
								table.Values = append(table.Values, nil)
								table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
								table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
							}
						}
					}
					rlNow, _ := schemaHandler.MaxRepetitionLevel(Common.StrToPath(pathStr))

					for j := ln - 1; j >= 0; j-- {
						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children["list"].Children["element"]
						newNode.Val = node.Val.Index(j).Elem()
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						newNode.DL = node.DL + 1

						newPathStr := newNode.PathMap.Path
						newSchemaIndex := schemaHandler.MapIndex[newPathStr]
						newInfo := schemaHandler.Infos[newSchemaIndex]
						if newInfo.RepetitionType == parquet.FieldRepetitionType_OPTIONAL { //element of LIST can only be optional or required
							newNode.DL++
						}

						stack = append(stack, newNode)
					}

				} else { //Repeated
					if ln <= 0 {
						for key, table := range res {
							if len(key) >= len(node.PathMap.Path) &&
								key[:len(node.PathMap.Path)] == node.PathMap.Path {
								table.Values = append(table.Values, nil)
								table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
								table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
							}
						}
					}
					rlNow, _ := schemaHandler.MaxRepetitionLevel(Common.StrToPath(pathStr))

					for j := ln - 1; j >= 0; j-- {
						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap
						newNode.Val = node.Val.Index(j).Elem()
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						newNode.DL = node.DL + 1
						stack = append(stack, newNode)
					}
				}

			} else {
				table := res[node.PathMap.Path]
				pT, cT := ParquetType.TypeNameToParquetType(info.Type, info.BaseType)
				val := ParquetType.JSONTypeToParquetType(node.Val, pT, cT, int(info.Length), int(info.Scale))

				table.Values = append(table.Values, val)
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
			}
		}
	}

	return &res, nil

}
