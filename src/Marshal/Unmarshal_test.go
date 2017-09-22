package Marshal

import (
	. "ParquetType"
	. "SchemaHandler"
	"fmt"
	"testing"
)

/*
type Student struct {
	Name    UTF8
	Age     INT32
	Classes []Class
}

type Class struct {
	Name   UTF8
	Number INT32
	Score  FLOAT
}
*/

func TestUnmarshal(t *testing.T) {
	schemaHandler := NewSchemaHandlerFromStruct(new(Student))

	clas := make([]Class, 3)
	clas[0].Name = "Math"
	clas[0].Number = 1
	clas[0].Score = 99.0
	clas[1].Name = "Physics"
	clas[1].Number = 2
	clas[1].Score = 98.0
	clas[2].Name = "Computer"
	clas[2].Number = 3
	clas[2].Score = 97.0

	stus := make([]Student, 3)
	stus[0].Name = "tong"
	stus[0].Age = 28
	stus[0].Classes = append(stus[0].Classes, clas[0])
	stus[0].Info = make(map[UTF8]UTF8)
	stus[0].Info["Country"] = "China"
	w := INT32(60)
	stus[0].Weight = &w

	stus[1].Name = "xitong"
	stus[1].Age = 27
	stus[1].Classes = append(stus[1].Classes, clas[:2]...)
	stus[1].Info = make(map[UTF8]UTF8)
	stus[1].Info["Country"] = "Eng"
	stus[1].Info["Address"] = "Road1"

	stus[2].Name = "ZhangXitong"
	stus[2].Age = 26
	stus[2].Classes = append(stus[2].Classes, clas...)
	stus[2].Info = make(map[UTF8]UTF8)
	stus[2].Info["Country"] = "USA"
	stus[2].Info["Address"] = "Road3"
	stus[2].Info["Height"] = "170"

	src := Marshal(stus, 0, len(stus), schemaHandler)
	for name, table := range *src {
		fmt.Println(name)
		fmt.Println("Val: ", table.Values)
		fmt.Println("RL: ", table.RepetitionLevels)
		fmt.Println("DL: ", table.DefinitionLevels)
	}

	dst := make([]Student, 0)
	Unmarshal(src, &dst, schemaHandler)

	fmt.Println(dst)

}
