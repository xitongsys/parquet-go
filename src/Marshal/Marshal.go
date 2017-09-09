package parquet_go

import (
	//"log"
	. "SchemaHandler"
	"reflect"
)

type Node struct {
	Val  reflect.Value
	Path []string
	RL   int32
	DL   int32
}

func Marshal(srcInterface interface{}, bgn int, end int, schemaHandler *SchemaHandler) *map[string]*Table {
	src := reflect.ValueOf(srcInterface)
	res := make(map[string]*Table)
	for i := 0; i < len(schemaHandler.SchemaElements); i++ {
		schema := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[i]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res[pathStr] = new(Table)
			res[pathStr].Path = StrToPath(pathStr)
			res[pathStr].MaxDefinitionLevel, _ = schemaHandler.MaxDefinitionLevel(res[pathStr].Path)
			res[pathStr].MaxRepetitionLevel, _ = schemaHandler.MaxRepetitionLevel(res[pathStr].Path)
			res[pathStr].Repetition_Type = schema.GetRepetitionType()
		}
	}

	rootName := schemaHandler.GetRootName()
	for i := bgn; i < end; i++ {
		node := new(Node)
		node.Val = src.Index(i)
		node.Path = append(node.Path, rootName)
		node.DL = -1
		node.RL = -1
		stack := make([]*Node, 0)
		stack = append(stack, node)

		for len(stack) > 0 {
		}
	}

}
