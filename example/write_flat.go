package main

import (
	. "ParquetType"
	. "SchemaHandler"
	. "Writer"
	"fmt"
	"log"
	"os"
)

type Student struct {
	Name   UTF8
	Age    INT32
	Id     INT64
	Weight FLOAT
	Sex    BOOLEAN
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
	var id int64 = 1
	for i := 0; i < len(stus); i++ {
		stus[i].Name = UTF8(stuName)
		stus[i].Age = INT32(i)
		stus[i].Id = INT64(i)
		stus[i].Weight = FLOAT(50.0 + float32(stus[i].Age)*0.1)
		stus[i].Sex = BOOLEAN(i%2 == 0)
		stuName = nextName(stuName)
		id++
	}
	return stus
}

func main() {
	stus := CreateStudents()
	schemaHandler := NewSchemaHandlerFromStruct(new(Student))
	file, _ := os.Create("flat.parquet")
	filetxt, _ := os.Create("flat.txt")
	defer file.Close()
	defer filetxt.Close()

	log.Println("Start Write Txt")
	for i := 0; i < len(stus); i++ {
		filetxt.WriteString(fmt.Sprintf("%v %v %v %v %v\n", stus[i].Name, stus[i].Age, stus[i].Id, stus[i].Weight, stus[i].Sex))
	}
	log.Println("Finish Write Txt")

	log.Println("Start Write Parquet")
	WriteTo(file, stus, schemaHandler)
	log.Println("Finish Write Parquet")
}
