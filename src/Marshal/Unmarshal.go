package Marshal

import (
	. "Common"
	. "SchemaHandler"
	"reflect"
)

type Node struct {
	Val  reflect.Value
	Path []string
	RL   int32
	DL   int32
}

//desInterface is a slice
func Marshal(tableMap *map[string]*Table, desInterface []interface{}, schemaHandler *SchemaHandler) {
	return nil
}
