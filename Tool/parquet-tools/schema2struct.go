package schema2struct

import (
	"fmt"

	"github.com/xitongsys/parquet-go/parquet"
)

func ParquetTypeToGoTypeStr(pT *parquet.ParquetType, cT *parquet.ConvertedType) string {
	res := ""
	switch *pT {
	case parquet.Type_BOOLEAN:
		res = "bool"
	case parquet.Type_INT32:
		res = "int32"
	case parquet.Type_INT64:
		res = "int64"
	case parquet.Type_INT96:
		res = "string"
	case parquet.Type_FLOAT:
		res = "float32"
	case parquet.Type_DOUBLE:
		res = "float64"
	case parquet.Type_BYTE_ARRAY:
		res = "string"
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		res = "string"
	}
	if cT != nil {
		switch *cT {
		case parquet.ConvertedType_UINT_8:
			res = "uint32"
		case parquet.ConvertedType_UINT_16:
			res = "uint32"
		case parquet.ConvertedType_UINT_32:
			res = "uint32"
		case parquet.ConvertedType_UINT_64:
			res = "uint64"
		}
	}
}

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
	pT, cT := self.SE.GetType(), self.SE.GetConvertedType()

	if pT == nil && cT == nil {

	} else if cT != nil && *cT == parquet.ConvertedType_MAP {

	} else if cT != nil && *cT == parquet.ConvertedType_LIST {

	} else {
		tagStr := "`parquet:\"name=%s, type=%s\"`"
		ptrStr := ""
		if self.SE.GetRepetitionType == parquet.FieldRepetitionType_OPTIONAL {
			ptrStr = "*"
		}
		goTypeStr := ParquetTypeToGoTypeStr(pT, cT)

		if cT != nil && *cT == parquet.ConvertedType_DECIMAL {
			if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				length := int(self.SE.GetTypeLength())
				tagStr = "`parquet:\"name=%s, type=%s, basetype=%s, length=%d\"`"
				res += " " + ptrStr + " " + goTypeStr + " " + fmt.Sprintf(tagStr, name, "DECIMAL", "FIXED_LEN_BYTE_ARRAY", length)

			} else {
				tagStr = "`parquet:\"name=%s, type=%s, basetype=%s\"`"
				res += " " + ptrStr + " " + goTypeStr + " " + fmt.Sprintf(tagStr, name, "DECIMAL", "", length)
			}

		} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			tagStr = "`parquet:\"name=%s, type=%s, length=%d\"`"
		} else {
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
