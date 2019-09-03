package schema

import (
	"reflect"
	"fmt"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/types"
)


// Get object type from schema by reflect
func (self *SchemaHandler) GetType() reflect.Type {
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
			pT := self.SchemaElements[idx].Type
			rT := self.SchemaElements[idx].RepetitionType
			
			if nc == 0 {
				if *rT != parquet.FieldRepetitionType_REPEATED {
					elementTypes[idx] = types.ParquetTypeToGoReflectType(pT, rT)

				} else {
					elementTypes[idx] = reflect.SliceOf(types.ParquetTypeToGoReflectType(pT, nil))
				}				
				
			} else {
				fields := []reflect.StructField{}
				for ci := range elements[idx] {
					fields = append(fields, reflect.StructField{
						Name: self.Infos[ci].InName,
						Type: elementTypes[ci],
					})
				}

				fmt.Println("==============", fields, elements[idx], nc, self.SchemaElements)
				structType := reflect.StructOf(fields)

				if *rT == parquet.FieldRepetitionType_REQUIRED {
					elementTypes[idx] = structType

				} else if *rT == parquet.FieldRepetitionType_OPTIONAL {
					elementTypes[idx] = reflect.New(structType).Type()

				} else if *rT == parquet.FieldRepetitionType_REPEATED {
					elementTypes[idx] = reflect.SliceOf(structType)
				}
			}

			stack = stack[:curlen]
		}
	}

	return elementTypes[0]
}
