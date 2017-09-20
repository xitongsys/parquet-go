package Marshal

import (
	. "Common"
	. "SchemaHandler"
	"reflect"
)

type KeyValue struct {
	Key   interface{}
	Value interface{}
}

type MapRecord struct {
	KeyValues []KeyValue
	Index     int
}

//desInterface is a slice
func Unmarshal(tableMap *map[string]*Table, desInterface []interface{}, schemaHandler *SchemaHandler) {
	ot := reflect.TypeOf(desInterface).Elem()
	tableIndex := make(map[string]int)

	flag := true
	for flag {
		flag = false
		obj := reflect.New(ot).Elem()
		mapRecord := make([reflect.Value]MapRecord)
		sliceRecord := make([reflect.Value]int)

		for name, table := range tableMap {
			ln := len(table.Values)
			path := table.Path

			for key, _ := range mapRecord {
				mapRecord[key].Index = 0
			}
			for key, _ := range sliceRecord {
				sliceRecord[key] = 0
			}

			for {
				rl, dl := 0, 0
				po := obj
				pathIndex := 0
				for pathIndex < len(path) {
					if po.Type().Kind() == reflect.Struct {
						if table.DefinitionLevels[tableIndex[name]] > dl {
							pathIndex++
							po = po.FieldByName(path[pathIndex])
							if po.Type().Kind() == reflect.Ptr {
								dl += 1
							}
						} else {
							break
						}

					} else if po.Type().Kind() == reflect.Slice {
						if _, ok := sliceRecord[po]; !ok {
							sliceRecord[po] = 0
						}

						if table.DefinitionLevels[tableIndex[name]] > dl {
							pathIndex += 1
							dl += 1
							rl += 1

							if rl >= table.RepetitionLevels[tableIndex[name]] {
								if sliceRecord[po] >= po.Len() {
									po = reflect.Append(po, reflect.New(po.Type().Elem()).Elem())
								}
								sliceRecord[po]++
								po = po.Index(sliceRecord[po] - 1)
							} else {
								po = po.Index(sliceRecord[po])
							}
							pathIndex += 1
						} else {
							break
						}

					} else if po.Type().Kind() == reflect.Map {
						if _, ok := mapRecord[po]; !ok {
							mapRecord[po] = MapRecord{KeyValues: make([]KeyValue, 0), Index: 0}
						}

						if table.DefinitionLevels[tableIndex[name]] > dl {
							if path[pathIndex+2] == "value" {
								pathIndex += 1
								dl += 1
								rl += 1

								if rl >= table.RepetitionLevels[tableIndex[name]] {
									if mapRecord[po].Index > len(mapRecord[po].KeyValues) {
										mapRecord[po].KeyValues = append(mapRecord[po].KeyValues,
											KeyValue{Key: nil, Value: nil})
									}
									mapRecord[po].Index++
									value := reflect.New(po.Type().Elem()).Elem()
									mapRecord[po].KeyValues[mapRecord[po].Index-1].Value = value
									po = value

								} else {
									po = mapRecord[po].KeyValues[mapRecord[po].Index]
								}

							} else if path[pathIndex+2] == "key" {
								if mapRecord[po].Index > len(mapRecord[po].KeyValues) {
									mapRecord[po].KeyValues = append(mapRecord[po].KeyValues,
										KeyValue{Key: nil, Value: nil})
								}
								mapRecord[po].Index++
								mapRecord[po].KeyValues[mapRecord[po].Index-1].Key = table.Values[tableIndex[name]]
								break
							}
						} else {
							break
						}

					} else if po.Type().Kind() == reflect.Ptr {
						if po.IsNil() {
							break
						} else {
							dl += 1
							po = po.Elem()
						}

					} else {
						po.Set(reflect.ValueOf(table.Values[tableIndex[name]]))
						break
					}
				}
				tableIndex[name]++
				if (tableIndex[name] < ln && table.DefinitionLevels[tableIndex[name]] == 0) ||
					(tableIndex[name] >= ln) {
					break
				}
			}
			if tableIndex[name] < ln {
				flag = true
			}
		}
	}

	return nil
}
