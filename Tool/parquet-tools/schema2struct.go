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
		tagStr := "`parquet:\"name=%s, type=%s\"`"
		switch cT {
		case parquet.ConvertedType_UTF8:
			res += " " + ptrStr + "string" + " " + fmt.Sprintf(tagStr, name, "UTF8")
		case parquet.ConvertedType_INT_8:
			res += " " + ptrStr + "int32" + " " + fmt.Sprintf(tagStr, name, "INT_8")
		case parquet.ConvertedType_INT_16:
			res += " " + ptrStr + "int32" + " " + fmt.Sprintf(tagStr, name, "INT_16")
		case parquet.ConvertedType_INT_32:
			res += " " + ptrStr + "int32" + " " + fmt.Sprintf(tagStr, name, "INT_32")
		case parquet.ConvertedType_INT_64:
			res += " " + ptrStr + "int64" + " " + fmt.Sprintf(tagStr, name, "INT_64")
		case parquet.ConvertedType_UINT_8:
			res += " " + ptrStr + "uint32" + " " + fmt.Sprintf(tagStr, name, "UINT_8")
		case parquet.ConvertedType_UINT_16:
			res += " " + ptrStr + "uint16" + " " + fmt.Sprintf(tagStr, name, "UINT_16")
		case parquet.ConvertedType_UINT_32:
			res += " " + ptrStr + "uint32" + " " + fmt.Sprintf(tagStr, name, "UINT_32")
		case parquet.ConvertedType_UINT_64:
			res += " " + ptrStr + "uint64" + " " + fmt.Sprintf(tagStr, name, "UINT_64")
		case parquet.ConvertedType_DATE:
			res += " " + ptrStr + "int32" + " " + fmt.Sprintf(tagStr, name, "DATE")
		case parquet.ConvertedType_TIME_MILLIS:
			res += " " + ptrStr + "int32" + " " + fmt.Sprintf(tagStr, name, "TIME_MILLIS")
		case parquet.ConvertedType_TIME_MICROS:
			res += " " + ptrStr + "int64" + " " + fmt.Sprintf(tagStr, name, "TIME_MICROS")
		case parquet.ConvertedType_TIMESTAMP_MILLIS:
			res += " " + ptrStr + "int64" + " " + fmt.Sprintf(tagStr, name, "TIMESTAMP_MILLIS")
		case parquet.ConvertedType_TIMESTAMP_MICROS:
			res += " " + ptrStr + "int64" + " " + fmt.Sprintf(tagStr, name, "TIMESTAMP_MICROS")
		case parquet.ConvertedType_INTERVAL:
			res += " " + ptrStr + "string" + " " + fmt.Sprintf(tagStr, name, "INTERVAL")
		case parquet.ConvertedType_DECIMAL:
			tagStr := "`parquet:\"name=%s, type=%s, scale=%d, precision=%d, basetype=%s\"`"
			scale, precision := int(self.SE.GetScale()), int(self.SE.GetPrecision())
			baseName := ""
			if pT == parquet.Type_INT32 {
				baseName = "INT32"
			} else if pT == parquet.Type_INT64 {
				baseName = "INT64"
			} else if pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				baseName = "FIXED_LEN_BYTE_ARRAY"
			} else if pT == parquet.Type_BYTE_ARRAY {
				baseName = "BYTE_ARRAY"
			}

			res += " " + ptrStr + "string" + " " + fmt.Sprintf(tagStr, name, "DECIMAL", scale, precision, baseName)

		case parquet.ConvertedType_MAP:
			keyNode := self.Children[0].Children[0]
			keyStr := keyNode.Output()[4:]
			if len(keyNode.Children) == 0 {

			}
			valNode := self.Children[0].Children[1]

		case parquet.ConvertedType_LIST:

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
