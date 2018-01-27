package Marshal

import (
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
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
type Marshaler interface {
	Marshal(node *Node, nodeBuf *NodeBufType) []*Node
}

type ParquetPtr struct{}

func (p *ParquetPtr) Marshal(node *Node, nodeBuf *NodeBufType) []*Node {
	nodes := make([]*Node, 0)
	if node.Val.IsNil() {
		return nodes
	} else {
		node.Val = node.Val.Elem()
		node.DL++
		nodes = append(nodes, node)
	}
	return nodes
}

type ParquetStruct struct{}

func (p *ParquetStruct) Marshal(node *Node, nodeBuf *NodeBufType) []*Node {
	numField := node.Val.Type().NumField()
	nodes := make([]*Node, numField)
	for j := 0; j < numField; j++ {
		tf := node.Val.Type().Field(j)
		name := tf.Name
		newNode := nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children[name]
		newNode.Val = node.Val.FieldByName(name)
		newNode.RL = node.RL
		newNode.DL = node.DL
		nodes[j] = newNode
	}
	return nodes
}

type ParquetSlice struct {
	schemaHandler *SchemaHandler.SchemaHandler
}

func (p *ParquetSlice) Marshal(node *Node, nodeBuf *NodeBufType) []*Node {
	nodes := make([]*Node, 0)
	ln := node.Val.Len()
	pathMap := node.PathMap
	path := node.PathMap.Path
	if *p.schemaHandler.SchemaElements[p.schemaHandler.MapIndex[node.PathMap.Path]].RepetitionType != parquet.FieldRepetitionType_REPEATED {
		pathMap = pathMap.Children["list"].Children["element"]
		path += ".list" + ".element"
	}
	if ln <= 0 {
		return nodes
	}

	rlNow, _ := p.schemaHandler.MaxRepetitionLevel(Common.StrToPath(path))
	for j := ln - 1; j >= 0; j-- {
		newNode := nodeBuf.GetNode()
		newNode.PathMap = pathMap
		newNode.Val = node.Val.Index(j)
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		newNode.DL = node.DL + 1
		nodes = append(nodes, newNode)
	}
	return nodes
}

type ParquetMap struct {
	schemaHandler *SchemaHandler.SchemaHandler
}

func (p *ParquetMap) Marshal(node *Node, nodeBuf *NodeBufType) []*Node {
	nodes := make([]*Node, 0)
	path := node.PathMap.Path + ".key_value"
	keys := node.Val.MapKeys()
	if len(keys) <= 0 {
		return nodes
	}

	rlNow, _ := p.schemaHandler.MaxRepetitionLevel(Common.StrToPath(path))
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
		nodes = append(nodes, newNode)

		newNode = nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["key_value"].Children["value"]
		newNode.Val = value
		newNode.DL = node.DL + 1
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		nodes = append(nodes, newNode)
	}
	return nodes
}

//Convert the objects to table map. srcInterface is a slice of objects
func Marshal(srcInterface interface{}, bgn int, end int, schemaHandler *SchemaHandler.SchemaHandler) (tb *map[string]*Layout.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	src := reflect.ValueOf(srcInterface)
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
			var m Marshaler

			if tk == reflect.Ptr {
				m = &ParquetPtr{}
			} else if tk == reflect.Struct {
				m = &ParquetStruct{}
			} else if tk == reflect.Slice {
				m = &ParquetSlice{schemaHandler: schemaHandler}
			} else if tk == reflect.Map {
				m = &ParquetMap{schemaHandler: schemaHandler}
			} else {
				table := res[node.PathMap.Path]
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				sele := schemaHandler.SchemaElements[schemaIndex]
				table.Values = append(table.Values, ParquetType.GoTypeToParquetType(node.Val.Interface(), sele.Type, sele.ConvertedType))
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				continue
			}

			nodes := m.Marshal(node, nodeBuf)
			if len(nodes) == 0 {
				res[node.PathMap.Path].Values = append(res[node.PathMap.Path].Values, nil)
				res[node.PathMap.Path].DefinitionLevels = append(res[node.PathMap.Path].DefinitionLevels, node.DL)
				res[node.PathMap.Path].RepetitionLevels = append(res[node.PathMap.Path].RepetitionLevels, node.RL)
			} else {
				for _, node := range nodes {
					stack = append(stack, node)
				}
			}
		}
	}

	return &res, nil
}
