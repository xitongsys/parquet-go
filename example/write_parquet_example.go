package main

import (
	"encoding/json"
	"fmt"
	"os"
	"parquet_go"
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

func JsonStudents() []Student {
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

func JsonClasses() {
	className := "AAAAA_CLASS"
	classes := make([]Class, 10)
	var score float32 = 0.1
	for j := 0; j < len(classes); j++ {
		classes[j].Name = className
		classes[j].Number = int64(j)
		classes[j].Score = score
		className = nextName(className)
		score += 0.1
	}
	jsonBuf, _ := json.Marshal(classes)
	fmt.Println(string(jsonBuf))
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
	stus := JsonStudents()
	schemaHandler := parquet_go.NewSchemaHandlerFromStruct(new(Student))
	file, _ := os.Create("t1.parquet")
	defer file.Close()
	parquet_go.WriteTo(file, stus, schemaHandler)
	ReadParquet("./t1.parquet")
}
