package main

import (
	. "github.com/xitongsys/parquet-go/ParquetHandler"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"log"
	"os"
	"strconv"
	//"runtime/pprof"
)

type Student struct {
	Name   UTF8
	Age    INT32
	Id     INT64
	Weight DOUBLE
	Sex    BOOLEAN
	School UTF8
}

type MyFile struct {
	file *os.File
}

func (self *MyFile) Create(name string) error {
	file, err := os.Create(name)
	self.file = file
	return err
}
func (self *MyFile) Open(name string) error {
	file, err := os.Open(name)
	self.file = file
	return err
}
func (self *MyFile) Seek(offset int, pos int) (int64, error) {
	return self.file.Seek(int64(offset), pos)
}

func (self *MyFile) Read(b []byte) (n int, err error) {
	return self.file.Read(b)
}

func (self *MyFile) Write(b []byte) (n int, err error) {
	return self.file.Write(b)
}

func (self *MyFile) Close() {
	self.file.Close()
}

func main() {
	/*
		cpuf, _ := os.Create("cpu.profile")
		pprof.StartCPUProfile(cpuf)
		defer pprof.StopCPUProfile()
	*/

	fname := os.Args[1]
	num,_ := strconv.Atoi(os.Args[2])

	log.Println(fname, num)

	var f ParquetFile
	f = &MyFile{}

	//write flat
	f.Create(fname)
	ph := NewParquetHandler()
	ph.WriteInit(f, new(Student), 10, 30)

	for i := 0; i < num; i++ {
		stu := Student{
			Name:   UTF8("StudentName"),
			Age:    INT32(18 + i%10),
			Id:     INT64(i),
			Weight: DOUBLE(60 + i%10),
			Sex:    BOOLEAN(i%2 == 0),
			School: UTF8("PKU"),
		}
		ph.Write(stu)

		if i%(num/100) == 0 {
			log.Println(i*100/num, "%")
		}
	}
	ph.WriteStop()
	log.Println("Write Finished")
	f.Close()
	/*
		memf, _ := os.Create("mem.profile")
		pprof.WriteHeapProfile(memf)
		memf.Close()
	*/

}
