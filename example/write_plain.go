package main

import (
	"fmt"
	"os"
	"parquet_go"
	"reflect"
)

type Student struct {
	Name   string
	Age    int32
	Id     int64
	Weight float32
	Sex    bool
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
		stus[i].Name = stuName
		stus[i].Age = (int32(i) % 30 + 30)
		stus[i].Id = id
		stus[i].Weight = 50.0 + float32(stus[i].Age)*0.1
		stuName = nextName(stuName)
		id++
	}
	return stus
}

func ReadParquet(fname string) {
	file, _ := os.Open(fname)
	defer file.Close()

	res := parquet_go.Reader(file)
	for _, v := range res {
		fmt.Println(v.Path)
		for i, v2 := range v.Values {
			if reflect.TypeOf(v2) == reflect.TypeOf([]uint8{}) {
				fmt.Print(string(v2.([]byte)))
			} else {
				fmt.Print(v2)
			}
			fmt.Printf(" %d %d\n", v.DefinitionLevels[i], v.RepetitionLevels[i])
		}
	}
}

func main() {
	stus := CreateStudents()
	schemaHandler := parquet_go.NewSchemaHandlerFromStruct(new(Student))
	file, _ := os.Create("plain.parquet")
	defer file.Close()
	parquet_go.WriteTo(file, stus, schemaHandler)
	ReadParquet("./plain.parquet")
}
