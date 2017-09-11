package main

import (
	. "SchemaHandler"
	. "Writer"
	"fmt"
	"os"
	"reflect"
)

type Class struct {
	Name   string
	Number int64
	Score  float32
}

type Student struct {
	Name    string
	Age     int32
	Classes []Class
}

func nextName(nameStr string) string {
	name := []byte(nameStr)
	ln := len(name)
	if name[0] >= 'a' && name[0] <= 'z' {
		for i := 0; i < ln; i++ {
			if name[i] >= 'z' {
				name[i] = 'a'
			} else {
				name[i] = byte(int(name[i]) + 1)
				break
			}
		}
	} else {
		for i := 0; i < ln; i++ {
			if name[i] >= 'Z' {
				name[i] = 'A'
			} else {
				name[i] = byte(int(name[i]) + 1)
				break
			}
		}
	}

	return string(name)
}

func CreateStudents() []Student {
	stus := make([]Student, 10)
	stuName := "aaaaa_STU"
	className := "AAAAA_CLASS"
	for i := 0; i < len(stus); i++ {
		stus[i].Name = stuName
		stus[i].Age = (int32(i) % 30)
		classNum := i % 5
		stus[i].Classes = make([]Class, classNum)
		for j := 0; j < classNum; j++ {
			stus[i].Classes[j].Name = className
			stus[i].Classes[j].Number = int64(j)
			stus[i].Classes[j].Score = 0.1
			className = nextName(className)
		}
		stuName = nextName(stuName)
	}
	return stus
}

func main() {
	stus := CreateStudents()
	schemaHandler := parquet_go.NewSchemaHandlerFromStruct(new(Student))
	file, _ := os.Create("nested.parquet")
	defer file.Close()
	parquet_go.WriteTo(file, stus, schemaHandler)
	ReadParquet("./nested.parquet")
}
