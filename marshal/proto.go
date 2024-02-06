package marshal

import (
	"errors"
	"reflect"
	"time"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
)

const ProtoSecondsName = "Seconds"
const ProtoNanosName = "Nanos"

type ParquetTimestamp struct{}

func (p *ParquetTimestamp) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) []*Node {
	mills := node.Val.FieldByName(ProtoSecondsName).Int()*1000 + node.Val.FieldByName(ProtoNanosName).Int()/(int64)(time.Millisecond)
	node.Val = reflect.ValueOf(mills)
	stack = append(stack, node)
	return stack
}

// Convert the objects to table map. srcInterface is a slice of objects
func MarshalProto(srcInterface []interface{}, schemaHandler *schema.SchemaHandler) (tb *map[string]*layout.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unkown error")
			}
		}
	}()

	src := reflect.ValueOf(srcInterface)
	res := setupTableMap(schemaHandler, len(srcInterface))
	pathMap := schemaHandler.PathMap
	nodeBuf := NewNodeBuf(1)

	for i := 0; i < len(schemaHandler.SchemaElements); i++ {
		schemaDefinition := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[int32(i)]
		numChildren := schemaDefinition.GetNumChildren()
		if numChildren == 0 {
			table := layout.NewEmptyTable()
			table.Path = common.StrToPath(pathStr)
			table.MaxDefinitionLevel, _ = schemaHandler.MaxDefinitionLevel(table.Path)
			table.MaxRepetitionLevel, _ = schemaHandler.MaxRepetitionLevel(table.Path)
			table.RepetitionType = schemaDefinition.GetRepetitionType()
			table.Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
			table.Info = schemaHandler.Infos[i]
			// Pre-size tables under the assumption that they'll be filled.
			table.Values = make([]interface{}, 0, len(srcInterface))
			table.DefinitionLevels = make([]int32, 0, len(srcInterface))
			table.RepetitionLevels = make([]int32, 0, len(srcInterface))
			res[pathStr] = table
		}
	}

	stack := make([]*Node, 0, 100)
	for i := 0; i < len(srcInterface); i++ {
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

			tk := reflect.Interface
			if node.Val.IsValid() {
				tk = node.Val.Type().Kind()
			}
			var m Marshaler

			if tk == reflect.Ptr || tk == reflect.UnsafePointer || tk == reflect.Interface || tk == reflect.Uintptr {
				m = &ParquetPtr{}
			} else if tk == reflect.Struct {
				if node.Val.Type() == schema.ProtoTimestampType {
					m = &ParquetTimestamp{}
				} else {
					m = &ParquetStruct{}
				}
			} else if tk == reflect.Slice || tk == reflect.Array {
				m = &ParquetSlice{schemaHandler: schemaHandler}
			} else if tk == reflect.Map {
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				sele := schemaHandler.SchemaElements[schemaIndex]
				if !sele.IsSetConvertedType() {
					m = &ParquetMapStruct{schemaHandler: schemaHandler}
				} else {
					m = &ParquetMap{schemaHandler: schemaHandler}
				}
			} else {
				table := res[node.PathMap.Path]
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				schemaDefinition := schemaHandler.SchemaElements[schemaIndex]
				var v interface{}
				if node.Val.IsValid() {
					v = node.Val.Interface()
				}
				// special handling for the enum
				if schemaDefinition.ConvertedType != nil && *schemaDefinition.ConvertedType == parquet.ConvertedType_ENUM {
					v = node.Val.MethodByName(schema.ProtoStringMethodName).Call(nil)[0].Interface().(string)
				}
				table.Values = append(table.Values, types.InterfaceToParquetType(v, schemaDefinition.Type))
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				continue
			}

			oldLen := len(stack)
			stack = m.Marshal(node, nodeBuf, stack)
			if len(stack) == oldLen {
				path := node.PathMap.Path
				index := schemaHandler.MapIndex[path]
				numChildren := schemaHandler.SchemaElements[index].GetNumChildren()
				if numChildren > int32(0) {
					for key, table := range res {
						if common.IsChildPath(path, key) {
							table.Values = append(table.Values, nil)
							table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
							table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
						}
					}
				} else {
					table := res[path]
					table.Values = append(table.Values, nil)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				}
			}
		}
	}

	return &res, nil
}
