package main

import (
	"fmt"
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type Student struct {
	Name    string               `parquet:"name=name, type=UTF8"`
	Age     int32                `parquet:"name=age, type=INT32"`
	Weight  *int32               `parquet:"name=weight, type=INT32"`
	Classes *map[string][]*Class `parquet:"name=classes, type=MAP, keytype=UTF8"`
}

type Class struct {
	Name     string   `parquet:"name=name, type=UTF8"`
	Id       *int32   `parquet:"name=id, type=INT32"`
	Required []string `parquet:"name=required, type=LIST, valuetype=UTF8"`
	Ignored  string
}

func (c Class) String() string {
	id := "nil"
	if c.Id != nil {
		id = fmt.Sprintf("%d", *c.Id)
	}
	res := fmt.Sprintf("{Name:%s, Id:%v, Required:%s}", c.Name, id, fmt.Sprint(c.Required))
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

func writeNested() {
	var err error
	math01ID := int32(1)
	math01 := Class{
		Name:     "Math1",
		Id:       &math01ID,
		Required: make([]string, 0),
	}

	math02ID := int32(2)
	math02 := Class{
		Name:     "Math2",
		Id:       &math02ID,
		Required: make([]string, 0),
	}
	math02.Required = append(math02.Required, "Math01")

	physics := Class{
		Name:     "Physics",
		Id:       nil,
		Required: make([]string, 0),
	}
	physics.Required = append(physics.Required, "Math01", "Math02")

	weight01 := int32(60)
	stu01Class := make(map[string][]*Class)
	stu01Class["Science1"] = make([]*Class, 0)
	stu01Class["Science1"] = append(stu01Class["Science"], &math01, &math02)
	stu01Class["Science2"] = make([]*Class, 0)
	stu01Class["Science2"] = append(stu01Class["Science"], &math01, &math02)
	stu01 := Student{
		Name:    "zxt",
		Age:     18,
		Weight:  &weight01,
		Classes: &stu01Class,
	}

	stu02Class := make(map[string][]*Class)
	stu02Class["Science"] = make([]*Class, 0)
	stu02Class["Science"] = append(stu02Class["Science"], &physics)
	stu02 := Student{
		Name:    "tong",
		Age:     29,
		Weight:  nil,
		Classes: &stu02Class,
	}

	stus := make([]Student, 0)
	stus = append(stus, stu01, stu02)

	//write nested
	fw, err := local.NewLocalFileWriter("nested.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	for _, stu := range stus {
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
			return
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	fw.Close()
	log.Println("Write Finished")

	//read nested
	fr, err := local.NewLocalFileReader("nested.parquet")
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num := int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}
	pr.ReadStop()
	fr.Close()
}

func main() {
	writeNested()
}
