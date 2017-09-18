package Marshal

import (
	. "Common"
	. "SchemaHandler"
	"reflect"
)

//desInterface is a slice
func Unmarshal(tableMap *map[string]*Table, desInterface []interface{}, schemaHandler *SchemaHandler) {
	ot := reflect.TypeOf(desInterface).Elem()

	bgns := make(map[string]int)
	names := make([]string, 0)
	for name, _ := range tableMap {
		bgns[name] = 0
		names = append(names, name)
	}

	for {
		val := reflect.New(ot)
		var dl, rl int32 = 0, 0
	}

	return nil
}
