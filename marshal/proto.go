package marshal

import (
	"reflect"
	"time"

	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/schema"
)

const ProtoSecondsName = "Seconds"
const ProtoNanosName = "Nanos"

type ParquetTimestamp struct{}

// Marshal the proto timestamp into milliseconds
func (p *ParquetTimestamp) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) []*Node {
	millis := node.Val.FieldByName(ProtoSecondsName).Int()*1000 + node.Val.FieldByName(ProtoNanosName).Int()/(int64)(time.Millisecond)
	node.Val = reflect.ValueOf(millis)
	stack = append(stack, node)
	return stack
}

// Convert the objects to table map. srcInterface is a slice of objects
func MarshalProto(srcInterface []interface{}, schemaHandler *schema.SchemaHandler) (tb *map[string]*layout.Table, err error) {
	return MarshalStruct(srcInterface, schemaHandler, true)
}
