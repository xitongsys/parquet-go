package main

import (
	"encoding/json"
	"fmt"
)

type Class struct {
	Name   string
	Number int
	Score  float32
}

type Student struct {
	Name    string
	Age     int
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

func JsonStudents() {
	stus := make([]Student, 10)
	stuName := "aaaaa_STU"
	className := "AAAAA_CLASS"
	for i := 0; i < len(stus); i++ {
		stus[i].Name = stuName
		stus[i].Age = i % 30
		classNum := i % 5
		stus[i].Classes = make([]Class, classNum)
		for j := 0; j < classNum; j++ {
			stus[i].Classes[j].Name = className
			stus[i].Classes[j].Number = j
			className = nextName(className)
		}
		stuName = nextName(stuName)
	}
	jsonBuf, _ := json.Marshal(stus)
	fmt.Println(string(jsonBuf))
}

func JsonClasses() {
	className := "AAAAA_CLASS"
	classes := make([]Class, 10)
	var score float32 = 0.1
	for j := 0; j < len(classes); j++ {
		classes[j].Name = className
		classes[j].Number = j
		classes[j].Score = score
		className = nextName(className)
		score += 0.1
	}
	jsonBuf, _ := json.Marshal(classes)
	fmt.Println(string(jsonBuf))
}

func main() {
	JsonClasses()
}
