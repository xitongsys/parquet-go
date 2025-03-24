package schematool

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

func ParquetTypeToParquetTypeStr(pT *parquet.Type, cT *parquet.ConvertedType) (string, string) {
	var pTStr, cTStr string
	if pT != nil {
		switch *pT {
		case parquet.Type_BOOLEAN:
			pTStr = "BOOLEAN"
		case parquet.Type_INT32:
			pTStr = "INT32"
		case parquet.Type_INT64:
			pTStr = "INT64"
		case parquet.Type_INT96:
			pTStr = "INT96"
		case parquet.Type_FLOAT:
			pTStr = "FLOAT"
		case parquet.Type_DOUBLE:
			pTStr = "DOUBLE"
		case parquet.Type_BYTE_ARRAY:
			pTStr = "BYTE_ARRAY"
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			pTStr = "FIXED_LEN_BYTE_ARRAY"
		}
	}
	if cT != nil {
		switch *cT {
		case parquet.ConvertedType_UTF8:
			cTStr = "UTF8"
		case parquet.ConvertedType_INT_8:
			cTStr = "INT_8"
		case parquet.ConvertedType_INT_16:
			cTStr = "INT_16"
		case parquet.ConvertedType_INT_32:
			cTStr = "INT_32"
		case parquet.ConvertedType_INT_64:
			cTStr = "INT_64"
		case parquet.ConvertedType_UINT_8:
			cTStr = "UINT_8"
		case parquet.ConvertedType_UINT_16:
			cTStr = "UINT_16"
		case parquet.ConvertedType_UINT_32:
			cTStr = "UINT_32"
		case parquet.ConvertedType_UINT_64:
			cTStr = "UINT_64"
		case parquet.ConvertedType_DATE:
			cTStr = "DATE"
		case parquet.ConvertedType_TIME_MILLIS:
			cTStr = "TIME_MILLIS"
		case parquet.ConvertedType_TIME_MICROS:
			cTStr = "TIME_MICROS"
		case parquet.ConvertedType_TIMESTAMP_MILLIS:
			cTStr = "TIMESTAMP_MILLIS"
		case parquet.ConvertedType_TIMESTAMP_MICROS:
			cTStr = "TIMESTAMP_MICROS"
		case parquet.ConvertedType_INTERVAL:
			cTStr = "INTERVAL"
		case parquet.ConvertedType_DECIMAL:
			cTStr = "DECIMAL"
		case parquet.ConvertedType_MAP:
			cTStr = "MAP"
		case parquet.ConvertedType_LIST:
			cTStr = "LIST"
		}
	}
	return pTStr, cTStr
}

func ParquetTypeToGoTypeStr(pT *parquet.Type, cT *parquet.ConvertedType) string {
	res := ""
	if pT != nil {
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
	}
	return res
}

type Node struct {
	Indent   string
	SE       *parquet.SchemaElement
	Children []*Node
}

func NewNode(schema *parquet.SchemaElement) *Node {
	node := &(Node{
		Indent:   "",
		SE:       schema,
		Children: []*Node{},
	})
	return node
}

func (n *Node) OutputJsonSchema() string {
	res := "{\"Tag\":"
	pT, cT := n.SE.Type, n.SE.ConvertedType
	rTStr := "REQUIRED"
	if n.SE.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		rTStr = "OPTIONAL"
	} else if n.SE.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		rTStr = "REPEATED"
	}

	pTStr, cTStr := ParquetTypeToParquetTypeStr(pT, cT)
	tagStr := "\"name=%s, type=%s, repetitiontype=%s\""

	name := n.SE.GetName()

	if len(n.Children) == 0 {
		if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY && cT == nil {
			length := n.SE.GetTypeLength()
			tagStr = "\"name=%s, type=%s, length=%d, repetitiontype=%s\""
			res += fmt.Sprintf(tagStr, name, pTStr, length, rTStr) + "}"

		} else if cT != nil && *cT == parquet.ConvertedType_DECIMAL {
			scale, precision := n.SE.GetScale(), n.SE.GetPrecision()
			if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
				length := n.SE.GetTypeLength()
				tagStr = "\"name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, length=%d, repetitiontype=%s\""
				res += fmt.Sprintf(tagStr, name, pTStr, cTStr, scale, precision, length, rTStr) + "}"
			} else {
				tagStr = "\"name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, repetitiontype=%s\""
				res += fmt.Sprintf(tagStr, name, pTStr, cTStr, scale, precision, rTStr) + "}"
			}

		} else {
			if cT != nil {
				tagStr := "\"name=%s, type=%s, convertedtype=%s, repetitiontype=%s\""
				res += fmt.Sprintf(tagStr, name, pTStr, cTStr, rTStr) + "}"

			} else {
				res += fmt.Sprintf(tagStr, name, pTStr, rTStr) + "}"
			}
		}
	} else {
		if cT != nil {
			tagStr = "\"name=%s, type=%s, repetitiontype=%s\""
			res += fmt.Sprintf(tagStr, name, cTStr, rTStr)
		} else {
			tagStr = "\"name=%s, repetitiontype=%s\""
			res += fmt.Sprintf(tagStr, name, rTStr)
		}
		res += ",\n"
		res += "\"Fields\":[\n"

		nodes := n.Children
		if cT != nil {
			nodes = n.Children[0].Children
		}

		for i := 0; i < len(nodes); i++ {
			cNode := nodes[i]
			if i == len(nodes)-1 {
				res += cNode.OutputJsonSchema() + "\n"
			} else {
				res += cNode.OutputJsonSchema() + ",\n"
			}
		}

		res += "]\n"
		res += "}"

	}
	return res
}

func GetTypeStr(pT *parquet.Type, cT *parquet.ConvertedType) string {
	if pT == nil && cT == nil {
		return "STRUCT"
	}
	pTStr, cTStr := ParquetTypeToParquetTypeStr(pT, cT)
	typeStr := pTStr
	if cT != nil {
		typeStr = cTStr
	}
	return typeStr
}

func (n *Node) getStructTags() string {
	rTStr := "REQUIRED"
	if n.SE.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		rTStr = "OPTIONAL"
	} else if n.SE.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		rTStr = "REPEATED"
	}

	pT, cT := n.SE.Type, n.SE.ConvertedType
	pTStr, cTStr := ParquetTypeToParquetTypeStr(pT, cT)
	typeStr := pTStr
	if cT != nil {
		typeStr = cTStr
	}
	tags := fmt.Sprintf("`parquet:\"name=%s, type=%s, repetitiontype=%s\"`", n.SE.Name, typeStr, rTStr)

	if pT == nil && cT == nil {
		tags = fmt.Sprintf("`parquet:\"name=%s, repetitiontype=%s\"`", n.SE.Name, rTStr)
	} else if cT != nil && *cT == parquet.ConvertedType_MAP && n.Children != nil {
		keyNode := n.Children[0].Children[0]
		keyTypeStr := GetTypeStr(keyNode.SE.Type, keyNode.SE.ConvertedType)
		valNode := n.Children[0].Children[1]
		valTypeStr := GetTypeStr(valNode.SE.Type, valNode.SE.ConvertedType)
		tags = fmt.Sprintf("`parquet:\"name=%s, type=MAP, repetitiontype=%s, keytype=%s, valuetype=%s\"`", n.SE.Name, rTStr, keyTypeStr, valTypeStr)

	} else if cT != nil && *cT == parquet.ConvertedType_LIST && n.Children != nil {
		cNode := n.Children[0].Children[0]
		valTypeStr := GetTypeStr(cNode.SE.Type, cNode.SE.ConvertedType)
		tags = fmt.Sprintf("`parquet:\"name=%s, type=LIST, repetitiontype=%s, valuetype=%s\"`", n.SE.Name, rTStr, valTypeStr)

	} else if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY && cT == nil {
		length := n.SE.GetTypeLength()
		tagStr := "`parquet:\"name=%s, type=%s, length=%d, repetitiontype=%s\"`"
		tags = fmt.Sprintf(tagStr, n.SE.Name, pTStr, length, rTStr)
	} else if cT != nil && *cT == parquet.ConvertedType_DECIMAL {
		scale, precision := n.SE.GetScale(), n.SE.GetPrecision()
		if *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			length := n.SE.GetTypeLength()
			tagStr := "`parquet:\"name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, length=%d, repetitiontype=%s\"`"
			tags = fmt.Sprintf(tagStr, n.SE.Name, pTStr, cTStr, scale, precision, length, rTStr)
		} else {
			tagStr := "`parquet:\"name=%s, type=%s, convertedtype=%s, scale=%d, precision=%d, repetitiontype=%s\"`"
			tags = fmt.Sprintf(tagStr, n.SE.Name, pTStr, n.SE.Type, scale, precision, rTStr)
		}
	}

	return tags
}

func Strip(s string) string {
	ln := len(s)
	i, j := ln-1, ln
	for i >= 0 && s[i] != '}' {
		if s[i] == '`' {
			j = i
		}
		i--
	}
	s = s[:j]
	return s
}

func (n *Node) OutputStruct(withName, withTags bool) string {
	name := n.SE.GetName()

	res := ""
	if withName {
		res += strings.Title(name)
	}

	pT, cT := n.SE.Type, n.SE.ConvertedType
	rTStr := " "
	if n.SE.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
		rTStr = " *"
	} else if n.SE.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		rTStr = " []"
	}
	if !withName {
		rTStr = rTStr[1:]
	}

	if pT == nil && cT == nil {
		res += rTStr + "struct {\n"
		for _, cNode := range n.Children {
			res += cNode.OutputStruct(true, withTags) + "\n"
		}
		res += "}"

	} else if cT != nil && *cT == parquet.ConvertedType_MAP && n.Children != nil {
		keyNode := n.Children[0].Children[0]
		keyPT, keyCT := keyNode.SE.Type, keyNode.SE.ConvertedType
		keyGoTypeStr := ParquetTypeToGoTypeStr(keyPT, keyCT)
		valNode := n.Children[0].Children[1]
		res += rTStr + "map[" + keyGoTypeStr + "]" + valNode.OutputStruct(false, withTags)
		res = Strip(res)

	} else if cT != nil && *cT == parquet.ConvertedType_LIST && n.Children != nil {
		cNode := n.Children[0].Children[0]
		res += rTStr + "[]" + cNode.OutputStruct(false, withTags)
		res = Strip(res)

	} else {
		goTypeStr := ParquetTypeToGoTypeStr(pT, cT)
		res += rTStr + goTypeStr
	}

	if withTags {
		res += " " + n.getStructTags() + "\n"
	}

	ress := strings.Split(res, "\n")
	for i := 0; i < len(ress); i++ {
		if i > 0 || withName {
			ress[i] = n.Indent + ress[i]
		}
	}

	res = strings.Join(ress, "\n")
	return res
}

type SchemaTree struct {
	Root *Node
}

func CreateSchemaTree(schemas []*parquet.SchemaElement) *SchemaTree {
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
			newNode.Indent += "  "
			node.Children = append(node.Children, newNode)
			stack = append(stack, newNode)
			pos++
		} else {
			stack = stack[:len(stack)-1]
		}
	}

	st := new(SchemaTree)
	st.Root = root
	return st
}

func (st *SchemaTree) OutputJsonSchema() string {
	jsonStr := st.Root.OutputJsonSchema()
	var obj schema.JSONSchemaItemType
	json.Unmarshal([]byte(jsonStr), &obj)
	res, _ := json.MarshalIndent(&obj, "", "  ")
	return string(res)
}

func (st *SchemaTree) OutputStruct(withTags bool) string {
	return st.Root.OutputStruct(true, withTags)
}
