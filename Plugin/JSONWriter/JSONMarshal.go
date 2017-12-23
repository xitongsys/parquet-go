package JSONWriter

import (
	"encoding/json"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/Marshal"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"reflect"
)

func JSONMarshal(ss []string, bgn int, end int, schemaHandler *SchemaHandler.SchemaHandler) *map[string]*Layout.Table {
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
			node, stack := stack[ln-1], stack[:ln-1]

			tk := node.Val.Type().Kind()
			pathStr := node.PathMap.Path
			schemaIndex := schemaHandler.MapIndex[pathStr]
			info := schemaHandler.Infos[schemaIndex]

			if info["repetitiontype"].(string) == "OPTIONAL" {
				node.DL++
			}

			if info["repetitiontype"].(string) == "REPEATED" {
				node.DL++
				node.RL++
			}

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
						value := node.Val.MapIndex(key)
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
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						stack = append(stack, newNode)
					}

				} else { //struct
					for j := 0; j < len(keys); j++ {
						key := keys[j]
						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children[key.String()]
						newNode.Val = node.Val.MapIndex(key)
						newNode.RL = node.RL
						newNode.DL = node.DL
						stack = append(stack, newNode)
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
						newNode.Val = node.Val.Index(j)
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						newNode.DL = node.DL + 1
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
						newNode.PathMap = node.PathMap.Children["list"].Children["element"]
						newNode.Val = node.Val.Index(j)
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
				val := JSONTypeToParquetType(node.Val, pT, cT)

				table.Values = append(table.Values, val)
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
			}
		}
	}

	return &res

}
