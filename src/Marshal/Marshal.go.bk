package parquet_go

import (
	//"log"
	"reflect"
)

type Node struct {
	Val  reflect.Value
	Path []string
	RL   int32
	DL   int32
}

func Marshal(srcInterface interface{}, bgn int, end int, schemaHandler *SchemaHandler) *map[string]*Table {
	src := reflect.ValueOf(srcInterface)

	res := make(map[string]*Table)
	for name, schema := range schemaHandler.SchemaMap {
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res[name] = new(Table)
			path := StrToPath(name)
			res[name].Path = path
			res[name].MaxDefinitionLevel, _ = schemaHandler.MaxDefinitionLevel(path)
			res[name].MaxRepetitionLevel, _ = schemaHandler.MaxRepetitionLevel(path)
			res[name].Repetition_Type = schema.GetRepetitionType()
		}
	}

	rootName := schemaHandler.RootName
	for i := bgn; i < end; i++ {
		stack := make([]*Node, 0)
		node := new(Node)
		node.Val = src.Index(i)
		node.Path = append(node.Path, rootName)
		node.RL = -1
		node.DL = -1
		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node := stack[ln-1]
			stack = stack[:ln-1]

			if node.Val.Type().Kind() == reflect.Struct {
				numField := TypeNumberField(node.Val.Type())
				for j := 0; int32(j) < numField; j++ {
					tf := node.Val.Type().Field(j)
					name := tf.Name
					newNode := new(Node)

					newNode.DL = node.DL
					newNode.RL = node.RL

					newNode.Path = append(newNode.Path, node.Path...)
					newNode.Path = append(newNode.Path, name)
					newNode.Val = node.Val.FieldByName(name)
					stack = append(stack, newNode)
				}

			} else if node.Val.Type().Kind() == reflect.Slice && node.Val.Type().Name()!="INT96"{
				ln := node.Val.Len()
				name := node.Path[len(node.Path)-1] + "_item"
				if ln <= 0 {
					newNode := new(Node)
					newNode.RL = node.RL
					dl, _ := schemaHandler.MaxDefinitionLevel(node.Path)
					newNode.DL = int32(dl)
					newNode.Val = reflect.New(node.Val.Type().Elem()).Elem()
					newNode.Path = append(newNode.Path, node.Path...)
					newNode.Path = append(newNode.Path, name)
					stack = append(stack, newNode)

				} else {
					for j := ln - 1; j >= 0; j-- {
						newNode := new(Node)
						newNode.Val = node.Val.Index(j)
						newNode.Path = append(newNode.Path, node.Path...)
						newNode.Path = append(newNode.Path, name)
						if j == 0 {
							if node.RL >= 0 {
								newNode.RL = node.RL
							} else {
								newNode.RL = 0
							}
						} else {
							rl, _ := schemaHandler.MaxRepetitionLevel(newNode.Path)
							newNode.RL = int32(rl)
						}
						dl, _ := schemaHandler.MaxDefinitionLevel(newNode.Path)
						newNode.DL = int32(dl)
						stack = append(stack, newNode)
					}
				}
			} else {
				dl, _ := schemaHandler.MaxDefinitionLevel(node.Path)
				maxDL := int32(dl)
				rl, _ := schemaHandler.MaxRepetitionLevel(node.Path)
				maxRL := int32(rl)

				if node.DL < 0 {
					node.DL = maxDL
				}
				if node.RL < 0 {
					node.RL = maxRL
				}

				//log.Println("----------", node.Val, node.DL, node.RL)

				pathStr := PathToStr(node.Path)
				res[pathStr].DefinitionLevels = append(res[pathStr].DefinitionLevels, int32(node.DL))
				res[pathStr].RepetitionLevels = append(res[pathStr].RepetitionLevels, int32(node.RL))
				res[pathStr].Values = append(res[pathStr].Values, node.Val.Interface())
			}
		}
	}

	return &res
}
