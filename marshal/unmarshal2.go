package marshal

import (
	//"errors"
	"reflect"
	"strings"
	"fmt"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
)

//Record Map KeyValue pair
type KeyValue struct {
	Key   reflect.Value
	Value reflect.Value
}

type MapRecord struct {
	KeyValues []KeyValue
	Index     int
}

//Convert the table map to objects slice. desInterface is a slice of pointers of objects
func Unmarshal(tableMap *map[string]*layout.Table, bgn int, end int, dstInterface interface{}, schemaHandler *schema.SchemaHandler, prefixPath string) (err error) {
	/*
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
	*/

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

	mapRecords := make(map[reflect.Value][]KeyValue)

	for name, table := range tableNeeds {
		path := table.Path
		bgn := tableBgn[name]
		end := tableEnd[name]
		schemaIndex := schemaHandler.MapIndex[common.PathToStr(path)]
		pT, cT := schemaHandler.SchemaElements[schemaIndex].Type, schemaHandler.SchemaElements[schemaIndex].ConvertedType

		repetitionLevels, definitionLevels := make([]int32, len(path)), make([]int32, len(path))
		for i := 0; i<len(path); i++ {
			repetitionLevels[i], _ = schemaHandler.MaxRepetitionLevel(path[:i+1])
			definitionLevels[i], _ = schemaHandler.MaxDefinitionLevel(path[:i+1])
		}
		repetitionIndexs := make([]int32, len(path))
		for i := 0; i < len(path); i++ {
			repetitionIndexs[i] = -1
		}

		fmt.Println("========", name)

		for i := bgn; i < end; i++ {
			rl, dl, val := table.RepetitionLevels[i], table.DefinitionLevels[i], table.Values[i]
			po, index := reflect.ValueOf(dstInterface).Elem(), 0

			for index < len(path) {
				if po.Type().Kind() == reflect.Slice {
					if po.IsNil() {
						po.Set(reflect.MakeSlice(po.Type(), 0, 0))
					}

					if rl == repetitionLevels[index] || repetitionIndexs[index] < 0 {
						repetitionIndexs[index]++
					}

					if repetitionIndexs[index] >= int32(po.Len()) {
						potmp := reflect.Append(po, reflect.New(po.Type().Elem()).Elem())
						po.Set(potmp)
					}

					fmt.Println("=====", po.Len(), repetitionIndexs[index])

					po = po.Index(int(repetitionIndexs[index]))

				} else if po.Type().Kind() == reflect.Map {
					if po.IsNil() {
						po.Set(reflect.MakeMap(po.Type()))
					}

					if _, ok := mapRecords[po]; !ok {
						mapRecords[po] = make([]KeyValue, 0)
					}

					index++
					if definitionLevels[index] > dl {
						break
					}

					if rl == repetitionLevels[index] {
						repetitionIndexs[index]++
					}

					if repetitionIndexs[index] >= int32(len(mapRecords[po])) {
						mapRecords[po] = append(mapRecords[po], KeyValue{})
					}

					if path[index + 1] == "key" {
						po = mapRecords[po][repetitionIndexs[index]].Key

					}else {
						po = mapRecords[po][repetitionIndexs[index]].Value
					}

					index++
					if definitionLevels[index] > dl {
						break
					}

				} else if po.Type().Kind() == reflect.Ptr {
					if po.IsNil() {
						po.Set(reflect.New(po.Type().Elem()))
					}

					po = po.Elem()

				} else if po.Type().Kind() == reflect.Struct {
					index++
					if definitionLevels[index] > dl {
						break;
					}
					po = po.FieldByName(path[index])

				} else {
					po.Set(reflect.ValueOf(types.ParquetTypeToGoType(val, pT, cT)))
					break
				}
			}
		}
	}

	for po, kvs := range mapRecords {
		for _, kv := range kvs {
			po.SetMapIndex(kv.Key, kv.Value)
		}
	}

	return nil
}
