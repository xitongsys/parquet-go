package Marshal

import (
	. "Common"
	. "SchemaHandler"
	"reflect"
)

//desInterface is a slice
func Unmarshal(tableMap *map[string]*Table, desInterface []interface{}, schemaHandler *SchemaHandler) {
	ot := reflect.TypeOf(desInterface).Elem()

	valIndex := make(map[string]int)
	for name, _ := range tableMap {
		bgns[name] = 0
	}

	flag := true
	for flag {
		flag = false
		val := reflect.New(ot)
		for name, table := range tableMap {
			for i := valIndex[name]; i < len(table.Values); i++ {
				var dl, rl int32 = 0, 0

				if i+1 < len(table.Values) && table.Values.DefinitionLevels[i+1] == 0 {
					valIndex[name] = i + 1
					flag = true
					break
				}
			}
		}

	}

	return nil
}
