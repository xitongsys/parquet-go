package JSONWriter

import (
	"encoding/json"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/Marshal"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
)

func MarshalJSON(ss []string, bgn int, end int, schemaHandler *SchemaHandler.SchemaHandler) *map[string]*Layout.Table {

	res := make(map[string]*Layout.Table)
	pathMap := schemaHandler.PathMap
	nodeBuf := Marshal.NewNodeBuf(1)

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

	stack := make([]*Marshal.Node, 0, 100)
	for i := bgn; i < end; i++ {
		stack = stack[:0]
		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		var ui interface{}
		json.Unmarshal([]byte(ss[i]), &ui)
		node.Val = reflect.ValueOf(ui)
		node.PathMap = pathMap

		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node = stack[ln-1]
			stack = stack[:ln-1]

			tk := node.Val.Type().Kind()

			pathStr := node.PathMap.Path
			schemaIndex := schemaHandler.MapIndex[pathStr]
			info := schemaHandler.Infos[schemaIndex]

			if tk == reflect.Map {
				keys := node.Val.MapKeys()

				if info["type"] == "MAP" { //real map
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
						if newInfo["repetitiontype"] == parquet.FieldRepetitionType_OPTIONAL { //map value only be :optional or required
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
						if _, ok := keysMap[key]; ok {
							newNode := nodeBuf.GetNode()
							newNode.PathMap = node.PathMap.Children[key]
							newNode.Val = node.Val.MapIndex(reflect.ValueOf(key)).Elem()
							newNode.RL = node.RL
							newNode.DL = node.DL
							newPathStr := newNode.PathMap.Path
							newSchemaIndex := schemaHandler.MapIndex[newPathStr]
							newInfo := schemaHandler.Infos[newSchemaIndex]
							if newInfo["repetitiontype"] == parquet.FieldRepetitionType_OPTIONAL {
								newNode.DL++
							}
							stack = append(stack, newNode)

						} else {
							newPathStr := node.PathMap.Children[key].Path
							for key, table := range res {
								if len(key) >= len(newPathStr) && key[:len(newPathStr)] == newPathStr {
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

				if info["type"] == "LIST" { //real LIST
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
						if newInfo["repetitiontype"] == parquet.FieldRepetitionType_OPTIONAL { //element of LIST can only be optional or required
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
						//newNode.DL = node.DL + 1
						stack = append(stack, newNode)
					}
				}

			} else {
				table := res[node.PathMap.Path]
				pT, cT := ParquetType.TypeNameToParquetType(info["type"].(string), info["basetype"].(string))
				val := JSONTypeToParquetType(node.Val, pT, cT, int(info["length"].(int32)), int(info["scale"].(int32)))

				table.Values = append(table.Values, val)
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
			}
		}
	}

	return &res

}
