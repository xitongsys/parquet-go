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
	Name   UTF8
	Age    INT32
	Id     INT64
	Weight FLOAT
	Sex    BOOLEAN
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
	Read("./flat.parquet")
}
