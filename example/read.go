package main

import (
	. "Marshal"
	. "ParquetType"
	. "Reader"
	. "SchemaHandler"
	"fmt"
	"os"
)

type Student struct {
	Name    UTF8
	Age     INT32
	Weight  *INT32
	Classes *map[UTF8][]*Class
}

type Class struct {
	Name     UTF8
	ID       *INT32
	Required []UTF8
}

func (c Class) String() string {
	id := "nil"
	if c.ID != nil {
		id = fmt.Sprintf("%d", *c.ID)
	}
	res := fmt.Sprintf("{Name:%s, ID:%v, Required:%s}", c.Name, id, fmt.Sprint(c.Required))
	return res
}

func (s Student) String() string {
	weight := "nil"
	if s.Weight != nil {
		weight = fmt.Sprintf("%d", *s.Weight)
	}

	cs := "{"
	for key, classes := range *s.Classes {
		s := string(key) + ":["
		for _, class := range classes {
			s += (*class).String() + ","
		}
		s += "]"
		cs += s
	}
	cs += "}"
	res := fmt.Sprintf("{Name:%s, Age:%d, Weight:%s, Classes:%s}", s.Name, s.Age, weight, cs)
	return res
}

func Read(fname string) {
	file, _ := os.Open(fname)
	defer file.Close()

	res := ReadParquet(file)
	schemaHandler := NewSchemaHandlerFromStruct(new(Student))
	for _, rowGroup := range res {
		tableMap := rowGroup.RowGroupToTableMap()
		stus := make([]Student, 0)
		Unmarshal(tableMap, &stus, schemaHandler)
		fmt.Println(stus)

	}
}

func main() {
	Read("./nested.parquet")
}
