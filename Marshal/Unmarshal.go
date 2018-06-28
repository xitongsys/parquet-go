package Marshal

import (
	"reflect"

	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Layout"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

type KeyValue struct {
	Key   reflect.Value
	Value reflect.Value
}

type MapRecord struct {
	KeyValues []KeyValue
	Index     int
}

//Convert the table map to objects slice. desInterface is a slice of pointers of objects
func Unmarshal(tableMap *map[string]*Layout.Table, bgn int, end int, dstInterface interface{}, schemaHandler *SchemaHandler.SchemaHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	tableIndex := make(map[string]int)
	tableBgn, tableEnd := make(map[string]int), make(map[string]int)

	tmpRes := reflect.ValueOf(dstInterface).Elem()

	for name, table := range *tableMap {
		ln := len(table.Values)
		num := -1
		tableBgn[name], tableEnd[name] = -1, -1
		for i := 0; i < ln; i++ {
			if table.RepetitionLevels[i] == 0 {
				num++
				if num == bgn {
					tableBgn[name] = i
					tableIndex[name] = i
				}
				if num == end {
					tableEnd[name] = i
					break
				}
			}
		}
		if tableEnd[name] < 0 {
			tableEnd[name] = ln
		}
		if tableBgn[name] < 0 {
			return
		}
	}

	flag := true
	for flag {
		flag = false
		obj := reflect.New(ot).Elem()
		mapRecord := make(map[reflect.Value]*MapRecord)
		sliceRecord := make(map[reflect.Value]int)

		for name, table := range *tableMap {
			path := table.Path
			end := tableEnd[name]
			schemaIndex := schemaHandler.MapIndex[Common.PathToStr(path)]
			pT, cT := schemaHandler.SchemaElements[schemaIndex].Type, schemaHandler.SchemaElements[schemaIndex].ConvertedType

			if tableIndex[name] >= end {
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
					curPathStr := Common.PathToStr(path[:pathIndex+1])

					if po.Type().Kind() == reflect.Struct {
						if (table.DefinitionLevels[tableIndex[name]] < table.MaxDefinitionLevel &&
							table.DefinitionLevels[tableIndex[name]] > int32(dl)) ||
							table.DefinitionLevels[tableIndex[name]] == table.MaxDefinitionLevel {
							pathIndex++
							//po = po.FieldByName(Common.HeadToUpper(path[pathIndex])) //HeadToUpper is for some filed is lowercase
							po = po.FieldByName(path[pathIndex])
						} else {
							break
						}

					} else if po.Type().Kind() == reflect.Slice &&
						*schemaHandler.SchemaElements[schemaHandler.MapIndex[curPathStr]].RepetitionType != parquet.FieldRepetitionType_REPEATED {
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

					} else if po.Type().Kind() == reflect.Slice &&
						*schemaHandler.SchemaElements[schemaHandler.MapIndex[curPathStr]].RepetitionType == parquet.FieldRepetitionType_REPEATED {
						if po.IsNil() {
							po.Set(reflect.MakeSlice(po.Type(), 0, 0))
						}
						if _, ok := sliceRecord[po]; !ok {
							sliceRecord[po] = -1
						}

						if table.DefinitionLevels[tableIndex[name]] > int32(dl) {
							pathIndex += 0
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
								mapRecord[po].KeyValues[mapRecord[po].Index].Key = reflect.ValueOf(ParquetType.ParquetTypeToGoType(table.Values[tableIndex[name]], pT, cT))
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
						po.Set(reflect.ValueOf(ParquetType.ParquetTypeToGoType(table.Values[tableIndex[name]], pT, cT)))
						break
					}
				} //for pathIndex < len(path)

				tableIndex[name]++
				if (tableIndex[name] < end && table.RepetitionLevels[tableIndex[name]] == 0) ||
					(tableIndex[name] >= end) {
					break
				}
			}
			if tableIndex[name] < end {
				flag = true
			}

		} //for name, table := range tableMap

		for m, record := range mapRecord {
			for _, kv := range record.KeyValues {
				m.SetMapIndex(kv.Key, kv.Value)
			}
		}

		tmpRes = reflect.Append(tmpRes, obj)

	}
	reflect.ValueOf(dstInterface).Elem().Set(tmpRes)
	return nil

}
