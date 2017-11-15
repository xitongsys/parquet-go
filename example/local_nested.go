package main

import (
	"fmt"
	"github.com/xitongsys/parquet-go/ParquetHandler"
	"github.com/xitongsys/parquet-go/ParquetType"
	"log"
	"os"
)

type MyFile struct {
	FilePath string
	File     *os.File
}

func (self *MyFile) Create(name string) (ParquetHandler.ParquetFile, error) {
	file, err := os.Create(name)
	myFile := new(MyFile)
	myFile.File = file
	return myFile, err

}
func (self *MyFile) Open(name string) (ParquetHandler.ParquetFile, error) {
	var (
		err error
	)
	if name == "" {
		name = self.FilePath
	}

	myFile := new(MyFile)
	myFile.FilePath = name
	myFile.File, err = os.Open(name)
	return myFile, err
}
func (self *MyFile) Seek(offset int, pos int) (int64, error) {
	return self.File.Seek(int64(offset), pos)
}

func (self *MyFile) Read(b []byte) (n int, err error) {
	return self.File.Read(b)
}

func (self *MyFile) Write(b []byte) (n int, err error) {
	return self.File.Write(b)
}

func (self *MyFile) Close() {
	self.File.Close()
}

type Student struct {
	Name    ParquetType.UTF8
	Age     ParquetType.INT32
	Weight  *ParquetType.INT32
	Classes *map[ParquetType.UTF8][]*Class
}

type Class struct {
	Name     ParquetType.UTF8
	Id       *ParquetType.INT32
	Required []ParquetType.UTF8
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
	math01ID := ParquetType.INT32(1)
	math01 := Class{
		Name:     "Math1",
		Id:       &math01ID,
		Required: make([]ParquetType.UTF8, 0),
	}

	math02ID := ParquetType.INT32(2)
	math02 := Class{
		Name:     "Math2",
		Id:       &math02ID,
		Required: make([]ParquetType.UTF8, 0),
	}
	math02.Required = append(math02.Required, "Math01")

	physics := Class{
		Name:     "Physics",
		Id:       nil,
		Required: make([]ParquetType.UTF8, 0),
	}
	physics.Required = append(physics.Required, "Math01", "Math02")

	weight01 := ParquetType.INT32(60)
	stu01Class := make(map[ParquetType.UTF8][]*Class)
	stu01Class["Science"] = make([]*Class, 0)
	stu01Class["Science"] = append(stu01Class["Science"], &math01, &math02)
	stu01 := Student{
		Name:    "zxt",
		Age:     18,
		Weight:  &weight01,
		Classes: &stu01Class,
	}

	stu02Class := make(map[ParquetType.UTF8][]*Class)
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

	var f ParquetHandler.ParquetFile
	f = &MyFile{}

	//write nested
	f, _ = f.Create("nested.parquet")
	ph := ParquetHandler.NewParquetHandler()
	ph.WriteInit(f, new(Student), 4, 30)
	for _, stu := range stus {
		ph.Write(stu)
	}
	ph.Flush()
	ph.WriteStop()
	f.Close()
	log.Println("Write Finished")

	//read nested
	f, _ = f.Open("nested.parquet")
	ph = ParquetHandler.NewParquetHandler()
	rowGroupNum := ph.ReadInit(f, 10)
	for i := 0; i < rowGroupNum; i++ {
		stus := make([]Student, 0)
		ph.ReadOneRowGroupAndUnmarshal(&stus)
		log.Println(stus)
	}
	f.Close()
}

func main() {
	writeNested()
}
