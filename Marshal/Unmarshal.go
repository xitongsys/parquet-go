package Marshal

import (
	. "Common"
	. "SchemaHandler"
	"reflect"
)

type KeyValue struct {
	Key   reflect.Value
	Value reflect.Value
}

type MapRecord struct {
	KeyValues []KeyValue
	Index     int
}

//desInterface is a slice ptr
func Unmarshal(tableMap *map[string]*Table, dstInterface interface{}, schemaHandler *SchemaHandler) {
	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	tableIndex := make(map[string]int)

	for name, _ := range *tableMap {
		tableIndex[name] = 0
	}

	flag := true
	for flag {
		flag = false
		obj := reflect.New(ot).Elem()
		mapRecord := make(map[reflect.Value]*MapRecord)
		sliceRecord := make(map[reflect.Value]int)

		for name, table := range *tableMap {
			ln := len(table.Values)
			path := table.Path

			if tableIndex[name] >= ln {
				continue
			}

			for key, _ := range mapRecord {
				mapRecord[key].Index = -1
			}
			for key, _ := range sliceRecord {
				sliceRecord[key] = -1
			}

			for {
				rl, dl := 0, 0
				po := obj
				pathIndex := 0
				for pathIndex < len(path) {
					if po.Type().Kind() == reflect.Struct {
						if (table.DefinitionLevels[tableIndex[name]] < table.MaxDefinitionLevel &&
							table.DefinitionLevels[tableIndex[name]] > int32(dl)) ||
							table.DefinitionLevels[tableIndex[name]] == table.MaxDefinitionLevel {
							pathIndex++
							po = po.FieldByName(path[pathIndex])

						} else {
							break
						}

					} else if po.Type().Kind() == reflect.Slice {
						if po.IsNil() {
							po.Set(reflect.MakeSlice(po.Type(), 0, 0))
						}
						if _, ok := sliceRecord[po]; !ok {
							sliceRecord[po] = -1
						}

						if table.DefinitionLevels[tableIndex[name]] > int32(dl) {
							pathIndex += 2
							dl += 1
							rl += 1
							if table.RepetitionLevels[tableIndex[name]] <= int32(rl) {
								sliceRecord[po]++
								if sliceRecord[po] >= po.Len() {
									potmp := reflect.Append(po, reflect.New(po.Type().Elem()).Elem())
									po.Set(potmp)
								}
								po = po.Index(sliceRecord[po])
							} else {
								po = po.Index(sliceRecord[po])
							}

						} else {
							break
						}

					} else if po.Type().Kind() == reflect.Map {
						if po.IsNil() {
							po.Set(reflect.MakeMap(po.Type()))
						}

						if _, ok := mapRecord[po]; !ok {
							mapRecord[po] = &MapRecord{KeyValues: make([]KeyValue, 0), Index: -1}
						}

						if table.DefinitionLevels[tableIndex[name]] > int32(dl) {
							if path[pathIndex+2] == "value" {
								pathIndex += 2
								dl += 1
								rl += 1

								if table.RepetitionLevels[tableIndex[name]] <= int32(rl) {
									mapRecord[po].Index++
									if mapRecord[po].Index >= len(mapRecord[po].KeyValues) {
										mapRecord[po].KeyValues = append(mapRecord[po].KeyValues,
											KeyValue{Key: reflect.ValueOf(nil), Value: reflect.ValueOf(nil)})
										value := reflect.New(po.Type().Elem()).Elem()
										mapRecord[po].KeyValues[mapRecord[po].Index].Value = value
									}
									if !mapRecord[po].KeyValues[mapRecord[po].Index].Value.IsValid() {
										mapRecord[po].KeyValues[mapRecord[po].Index].Value = reflect.New(po.Type().Elem()).Elem()
									}
									po = mapRecord[po].KeyValues[mapRecord[po].Index].Value

								} else {
									po = mapRecord[po].KeyValues[mapRecord[po].Index].Value
								}

							} else if path[pathIndex+2] == "key" {
								mapRecord[po].Index++
								if mapRecord[po].Index >= len(mapRecord[po].KeyValues) {
									mapRecord[po].KeyValues = append(mapRecord[po].KeyValues,
										KeyValue{Key: reflect.ValueOf(nil), Value: reflect.ValueOf(nil)})
								}
								mapRecord[po].KeyValues[mapRecord[po].Index].Key = reflect.ValueOf(table.Values[tableIndex[name]])
								break
							}
						} else {
							break
						}

					} else if po.Type().Kind() == reflect.Ptr {
						dl += 1
						if int32(dl) > table.DefinitionLevels[tableIndex[name]] {
							break
						}
						if po.IsNil() {
							po.Set(reflect.New(po.Type().Elem()))
						}
						po = po.Elem()

					} else {
						po.Set(reflect.ValueOf(table.Values[tableIndex[name]]))
						break
					}
				} //for pathIndex < len(path) {

				tableIndex[name]++
				if (tableIndex[name] < ln && table.RepetitionLevels[tableIndex[name]] == 0) ||
					(tableIndex[name] >= ln) {
					break
				}
			}
			if tableIndex[name] < ln {
				flag = true
			}

		} //for name, table := range tableMap

		for m, record := range mapRecord {
			for _, kv := range record.KeyValues {
				m.SetMapIndex(kv.Key, kv.Value)
			}
		}

		tmp := reflect.Append(
			reflect.ValueOf(dstInterface).Elem(),
			obj)
		reflect.ValueOf(dstInterface).Elem().Set(tmp)
	}

}
