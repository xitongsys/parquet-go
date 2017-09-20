package main

import (
	. "ParquetType"
	. "SchemaHandler"
	. "Writer"
	"log"
	"os"
)

type Class struct {
	Name   UTF8
	Number INT64
	Score  FLOAT
}

type Student struct {
	Name    UTF8
	Age     INT32
	Classes []Class
	MapTest map[UTF8]Class
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
		stus[i].Name = UTF8(stuName)
		stus[i].Age = INT32(int32(i) % 30)
		classNum := i % 5
		stus[i].Classes = make([]Class, classNum)
		for j := 0; j < classNum; j++ {
			stus[i].Classes[j].Name = UTF8(className)
			stus[i].Classes[j].Number = INT64(i + 10)
			stus[i].Classes[j].Score = 0.1
			className = nextName(className)
		}
		stus[i].MapTest = make(map[UTF8]Class)
		stus[i].MapTest[UTF8(stuName)] = Class{Name: UTF8("Name"), Number: INT64(1), Score: FLOAT(99.9)}
		stuName = nextName(stuName)
	}
	return stus
}

func main() {
	stus := CreateStudents()
	schemaHandler := NewSchemaHandlerFromStruct(new(Student))
	file, _ := os.Create("nested.parquet")
	log.Println("create file done")
	defer file.Close()
	WriteTo(file, stus, schemaHandler)
}
