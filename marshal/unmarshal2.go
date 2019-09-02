package marshal

import (
	"errors"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/types"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/parquet"
)

//Convert the table map to objects slice. desInterface is a slice of pointers of objects
func Unmarshal2(tableMap *map[string]*layout.Table, bgn int, end int, dstInterface interface{}, schemaHandler *schema.SchemaHandler, prefixPath string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown error")
			}
		}
	}()

	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	tableNeeds := make(map[string]*layout.Table)
	tableBgn, tableEnd := make(map[string]int), make(map[string]int)
	for name, table := range *tableMap {
		if !strings.HasPrefix(name, prefixPath) {
			continue
		}

		tableNeeds[name] = table

		ln := len(table.Values)
		num := -1
		tableBgn[name], tableEnd[name] = -1, -1
		for i := 0; i < ln; i++ {
			if table.RepetitionLevels[i] == 0 {
				num++
				if num == bgn {
					tableBgn[name] = i
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

	tmpRes := reflect.ValueOf(dstInterface).Elem()
	for name, table := range tableNeeds {
		path := table.Path
		bgn := tableBgn[name]
		end := tableEnd[name]
		schemaIndex := schemaHandler.MapIndex[common.PathToStr(path)]
		pT, cT := schemaHandler.SchemaElements[schemaIndex].Type, schemaHandler.SchemaElements[schemaIndex].ConvertedType
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)
		repObjIndex := make([]int32, maxRepetitionLevel + 1)
		repPathIndex := make([]int32, maxRepetitionLevel + 1)
		for i := int32(0); i <= maxRepetitionLevel; i++ {
			repPathIndex[i], err = schemaHandler.GetRepetitionLevelIndex(path, i)
			if err != nil {
				return err
			}
		}

		for i := bgn; i < end; i++ {
			rl, dl, val := table.RepetitionLevels[i], table.DefinitionLevels[i], table.Values[i]
			index, ov, pi := -1, reflect.ValueOf(dstInterface).Elem(), repPathIndex[rl]

			for {
				curPathStr := common.PathToStr(path[:index+1])

				if ov.Type().Kind() == reflect.Struct {
					index++
					ov = ov.FieldByName(path[index])

				} else if ov.Type().Kind() == reflect.Slice && 
				*schemaHandler.SchemaElements[schemaHandler.MapIndex[curPathStr]].RepetitionType != parquet.FieldRepetitionType_REPEATED {
					if ov.IsNil() {
						ov.Set(reflect.MakeSlice(ov.Type(), 0, 0))
					}

					if ov.Len() <= int(repObjIndex[rl])
				}

				if pi - 1 == int32(index) {
					if ov.Len() <= int(repObjIndex[rl]) {
						ot := ov.Elem().Type()
						ov.Set(reflect.Append(ov, reflect.New(ot).Elem()))
					}
					ov = ov.Index(int(repObjIndex[rl]))
				}
			}
			
		}

		for {
			rl, dl := 0, 0
			po := obj
			pathIndex := 0
			for pathIndex < len(path) {
				curPathStr := common.PathToStr(path[:pathIndex+1])

				if po.Type().Kind() == reflect.Struct {
					if (table.DefinitionLevels[tableIndex[name]] < table.MaxDefinitionLevel &&
						table.DefinitionLevels[tableIndex[name]] > int32(dl)) ||
						table.DefinitionLevels[tableIndex[name]] == table.MaxDefinitionLevel {
						pathIndex++
						//po = po.FieldByName(common.HeadToUpper(path[pathIndex])) //HeadToUpper is for some filed is lowercase
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
							mapRecord[po].KeyValues[mapRecord[po].Index].Key = reflect.ValueOf(types.ParquetTypeToGoType(table.Values[tableIndex[name]], pT, cT))
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
					po.Set(reflect.ValueOf(types.ParquetTypeToGoType(table.Values[tableIndex[name]], pT, cT)))
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

	
	reflect.ValueOf(dstInterface).Elem().Set(tmpRes)
	return nil

}
