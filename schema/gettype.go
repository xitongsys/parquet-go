package schema

import (
	"reflect"
	"fmt"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/types"
)


// Get object type from schema by reflect
func (self *SchemaHandler) GetTypes() []reflect.Type {
	ln := int32(len(self.SchemaElements))
	elements := make([][]int32, ln)
	for i := 0; i<int(ln); i++ {
		elements[i] = []int32{}
	}

	elementTypes := make([]reflect.Type, ln)

	var pos int32 = 0
	stack := make([][2]int32, 0) //stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
				idx := stack[len(stack)-1][0]
				elements[idx] = append(elements[idx], pos)
			}
			item := [2]int32{pos, self.SchemaElements[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++

		} else {
			curlen := len(stack) - 1
			idx := stack[curlen][0]
			nc := self.SchemaElements[idx].GetNumChildren()
			pT, cT := self.SchemaElements[idx].Type, self.SchemaElements[idx].ConvertedType
			rT := self.SchemaElements[idx].RepetitionType
			
			if nc == 0 {
				if *rT != parquet.FieldRepetitionType_REPEATED {
					elementTypes[idx] = types.ParquetTypeToGoReflectType(pT, rT)

				} else {
					elementTypes[idx] = reflect.SliceOf(types.ParquetTypeToGoReflectType(pT, nil))
				}				
				
			} else {
				if cT != nil && *cT == parquet.ConvertedType_LIST &&
					len(elements[idx]) == 1 && 
					self.GetInName(int(elements[idx][0])) == "List" &&
					len(elements[elements[idx][0]]) == 1 && 
					self.GetInName(int(elements[elements[idx][0]][0])) == "Element" {
						cidx := elements[elements[idx][0]][0]
						elementTypes [idx] = reflect.SliceOf(elementTypes[cidx])
					
				} else if cT != nil && *cT == parquet.ConvertedType_MAP && 
					len(elements[idx]) == 1 && 
					self.GetInName(int(elements[idx][0])) == "Key_value" &&
					len(elements[elements[idx][0]]) == 2 && 
					self.GetInName(int(elements[elements[idx][0]][0])) == "Key" && 
					self.GetInName(int(elements[elements[idx][0]][1])) == "Value"{
						kIdx, vIdx := elements[elements[idx][0]][0], elements[elements[idx][0]][1]
						kT, vT := elementTypes[kIdx], elementTypes[vIdx]
						elementTypes[idx] = reflect.MapOf(kT, vT)

				}else {
					fields := []reflect.StructField{}
					for _, ci := range elements[idx] {
						fields = append(fields, reflect.StructField{
							Name: self.Infos[ci].InName,
							Type: elementTypes[ci],
						})
					}

					structType := reflect.StructOf(fields)

					if rT == nil || *rT == parquet.FieldRepetitionType_REQUIRED {
						elementTypes[idx] = structType

					} else if *rT == parquet.FieldRepetitionType_OPTIONAL {
						elementTypes[idx] = reflect.New(structType).Type()

					} else if *rT == parquet.FieldRepetitionType_REPEATED {
						elementTypes[idx] = reflect.SliceOf(structType)
					}
				}
			}

			stack = stack[:curlen]
		}
	}

	return elementTypes
}

func (self *SchemaHandler) GetType(prefixPath string) (reflect.Type, error) {
	prefixPath, err := self.ConvertToInPathStr(prefixPath)
	if err != nil {
		return nil, err
	}

	ts := self.GetTypes()
	if idx, ok := self.MapIndex[prefixPath]; !ok {
		return nil, fmt.Errorf("[GetType] Can't find %v", prefixPath)
	} else {
		return ts[idx], nil
	}
}
