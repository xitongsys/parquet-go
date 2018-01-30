package schema2struct

import (
	"fmt"

	"github.com/xitongsys/parquet-go/parquet"
)

type Node struct {
	SE       *parquet.SchemaElement
	Children []*Node
}

func NewNode(schema *parquet.SchemaElement) *Node {
	node := &(Node{
		SE:       nil,
		Children: []*Node{},
	})
	return node
}

func (self *Node) Output() string {
	name := self.SE.GetName()
	res := name
	pT, cT = self.SE.GetType(), self.SE.GetConvertedType()

	if pT == nil && cT == nil {

	} else if cT == nil {
		tagStr := "`parquet:\"name=%s, type=%s\"`"
		ptrStr := ""
		if self.SE.GetRepetitionType == parquet.FieldRepetitionType_OPTIONAL {
			ptrStr = "*"
		}
		switch pT {
		case parquet.Type_BOOLEAN:
			res += " " + ptrStr + "bool" + " " + fmt.Sprintf(tagStr, name, "BOOLEAN")
		case parquet.Type_INT32:
			res += " " + ptrStr + "int32" + " " + fmt.Sprintf(tagStr, name, "INT32")
		case parquet.Type_INT64:
			res += " " + ptrStr + "int64" + " " + fmt.Sprintf(tagStr, name, "INT64")
		case parquet.Type_INT96:
			res += " " + ptrStr + "string" + " " + fmt.Sprintf(tagStr, name, "INT96")
		case parquet.Type_FLOAT:
			res += " " + ptrStr + "float32" + " " + fmt.Sprintf(tagStr, name, "FLOAT")
		case parquet.Type_DOUBLE:
			res += " " + ptrStr + "float64" + " " + fmt.Sprintf(tagStr, name, "DOUBLE")
		case parquet.Type_BYTE_ARRAY:
			res += " " + ptrStr + "string" + " " + fmt.Sprintf(tagStr, name, "BYTE_ARRAY")
		case parquet.Type.Type_FIXED_LEN_BYTE_ARRAY:
			tagStr := "`parquet:\"name=%s, type=%s, length=%d\"`"
			length := int(self.SE.GetTypeLength())
			res += " " + ptrStr + "string" + " " + fmt.Sprintf(tagStr, name, "FIXED_LEN_BYTE_ARRAY", length)
		}

	} else {
		switch cT {
		case parquet.ConvertedType_UTF8:

		case parquet.ConvertedType_INT_8:
		case parquet.ConvertedType_INT_16:
		case parquet.ConvertedType_INT_32:
		case parquet.ConvertedType_INT_64:
		case parquet.ConvertedType_UINT_8:
		case parquet.ConvertedType_UINT_16:
		case parquet.ConvertedType_UINT_32:
		case parquet.ConvertedType_UINT_64:
		case parquet.ConvertedType_DATE:
		case parquet.ConvertedType_TIME_MILLIS:
		case parquet.ConvertedType_TIME_MICROS:
		case parquet.ConvertedType_TIMESTAMP_MILLIS:
		case parquet.ConvertedType_TIMESTAMP_MICROS:
		case parquet.ConvertedType_INTERVAL:
		case parquet.ConvertedType_DECIMAL:
		}
	}
	return res

}

func CreateTree(schemas []*parquet.SchemaElement) *Node {
	ln := len(schemas)
	pos := 0
	stack := make([]*Node, 0)
	root := NewNode(schemas[0])
	stack = append(stack, root)
	pos++

	for len(stack) > 0 {
		node := stack[len(stack)-1]
		numChildren := int(node.SE.GetNumChildren())
		lc := len(node.Children)
		if lc < numChildren {
			newNode := NewNode(schemas[pos])
			node.Children = append(node.Children, newNode)
			stack = append(stack, newNode)
			pos++
		} else {
			stack = stack[:len(stack)-1]
		}
	}
	return root

}
