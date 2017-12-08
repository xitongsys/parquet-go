package Marshal

import (
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"reflect"
)

type Node struct {
	Val     reflect.Value
	PathMap *SchemaHandler.PathMapType
	RL      int32
	DL      int32
}

//Improve Performance///////////////////////////
//NodeBuf
type NodeBufType struct {
	Index int
	Buf   []*Node
}

func NewNodeBuf(ln int) *NodeBufType {
	nodeBuf := new(NodeBufType)
	nodeBuf.Index = 0
	nodeBuf.Buf = make([]*Node, ln)
	for i := 0; i < ln; i++ {
		nodeBuf.Buf[i] = new(Node)
	}
	return nodeBuf
}

func (self *NodeBufType) GetNode() *Node {
	if self.Index >= len(self.Buf) {
		self.Buf = append(self.Buf, new(Node))
	}
	self.Index++
	return self.Buf[self.Index-1]
}

func (self *NodeBufType) Reset() {
	self.Index = 0
}

////////for improve performance///////////////////////////////////

//Convert the objects to table map. srcInterface is a slice of objects
func Marshal(srcInterface interface{}, bgn int, end int, schemaHandler *SchemaHandler.SchemaHandler) *map[string]*Layout.Table {
	src := reflect.ValueOf(srcInterface)
	res := make(map[string]*Layout.Table)
	pathMap := schemaHandler.PathMap
	nodeBuf := NewNodeBuf(1)

	for i := 0; i < len(schemaHandler.SchemaElements); i++ {
		schema := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res[pathStr] = new(Layout.Table)
			res[pathStr].Path = Common.StrToPath(pathStr)
			res[pathStr].MaxDefinitionLevel, _ = schemaHandler.MaxDefinitionLevel(res[pathStr].Path)
			res[pathStr].MaxRepetitionLevel, _ = schemaHandler.MaxRepetitionLevel(res[pathStr].Path)
			res[pathStr].RepetitionType = schema.GetRepetitionType()
			res[pathStr].Type = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]].GetType()
		}
	}

	stack := make([]*Node, 0, 100)
	for i := bgn; i < end; i++ {
		stack = stack[:0]
		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		node.Val = src.Index(i)
		if src.Index(i).Type().Kind() == reflect.Interface {
			node.Val = src.Index(i).Elem()
		}
		node.PathMap = pathMap
		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node := stack[ln-1]
			stack = stack[:ln-1]

			tk := node.Val.Type().Kind()

			if tk == reflect.Ptr {
				if node.Val.IsNil() {
					for key, table := range res {
						path := node.PathMap.Path
						if len(key) >= len(path) && key[:len(path)] == path {
							table.Values = append(table.Values, nil)
							table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
							table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
						}
					}
				} else {
					node.Val = node.Val.Elem()
					node.DL++
					stack = append(stack, node)
				}
			} else if tk == reflect.Struct {
				numField := node.Val.Type().NumField()
				for j := 0; j < numField; j++ {
					tf := node.Val.Type().Field(j)
					name := tf.Name
					newNode := nodeBuf.GetNode()
					newNode.PathMap = node.PathMap.Children[name]
					newNode.Val = node.Val.FieldByName(name)
					newNode.RL = node.RL
					newNode.DL = node.DL
					stack = append(stack, newNode)
				}
			} else if tk == reflect.Slice {
				ln := node.Val.Len()
				path := node.PathMap.Path + ".list" + ".element"

				if ln <= 0 {
					for key, table := range res {
						if len(key) >= len(node.PathMap.Path) && key[:len(node.PathMap.Path)] == node.PathMap.Path {
							table.Values = append(table.Values, nil)
							table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
							table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
						}
					}
				}

				rlNow, _ := schemaHandler.MaxRepetitionLevel(Common.StrToPath(path))

				for j := ln - 1; j >= 0; j-- {
					newNode := nodeBuf.GetNode()
					newNode.PathMap = node.PathMap.Children["list"].Children["element"]
					newNode.Val = node.Val.Index(j)
					if j == 0 {
						newNode.RL = node.RL
					} else {
						//newNode.RL = node.RL + 1
						newNode.RL = rlNow
					}
					newNode.DL = node.DL + 1 //list is repeated
					stack = append(stack, newNode)
				}
			} else if tk == reflect.Map {
				path := node.PathMap.Path + ".key_value"
				keys := node.Val.MapKeys()
				if len(keys) <= 0 {
					for key, table := range res {
						if len(key) >= len(node.PathMap.Path) && key[:len(node.PathMap.Path)] == node.PathMap.Path {
							table.Values = append(table.Values, nil)
							table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
							table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
						}
					}
				}

				rlNow, _ := schemaHandler.MaxRepetitionLevel(Common.StrToPath(path))
				rlNow += 1

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
						//newNode.RL = node.RL + 1
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
						//newNode.RL = node.RL + 1
						newNode.RL = rlNow
					}
					stack = append(stack, newNode)

				}
			} else {
				table := res[node.PathMap.Path]
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				sele := schemaHandler.SchemaElements[schemaIndex]
				table.Values = append(table.Values, ParquetType.GoTypeToParquetType(node.Val.Interface(), sele.Type, sele.ConvertedType))
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)

			}
		}
	}
	return &res
}
