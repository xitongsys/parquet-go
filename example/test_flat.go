package main

import (
	//. "github.com/xitongsys/parquet-go/Marshal"
	. "github.com/xitongsys/parquet-go/ParquetHandler"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"log"
	"os"
	"runtime/pprof"
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
	cpuf, _ := os.Create("cpu.profile")
	pprof.StartCPUProfile(cpuf)
	defer pprof.StopCPUProfile()

	var f ParquetFile
	f = &MyFile{}

	//write flat
	f.Create("flat.parquet")
	ph := NewParquetHandler()
	ph.WriteInit(f, new(Student), 40)

	num := 100000000
	id := 1
	stuName := "aaaaaaaaaa"

	for i := 0; i < num; i++ {
		stu := Student{
			Name:   UTF8(stuName),
			Age:    INT32(i),
			Id:     INT64(id),
			Weight: FLOAT(50.0 + float32(i)*0.1),
			Sex:    BOOLEAN(i%2 == 0),
		}
		stuName = nextName(stuName)
		id++
		ph.Write(stu)

		if i%(num/100) == 0 {
			log.Println(i*100/num, "%")
		}
	}
	ph.WriteStop()
	log.Println("Write Finished")
	f.Close()

	memf, _ := os.Create("mem.profile")
	pprof.WriteHeapProfile(memf)
	memf.Close()

	/*

		///read flat
		f.Open("flat.parquet")
		ph = NewParquetHandler()
		rowGroupNum := ph.ReadInit(f)
		for i := 0; i < rowGroupNum; i++ {
			stus := make([]Student, 0)
			tmap := ph.ReadOneRowGroup()
			Unmarshal(tmap, &stus, ph.SchemaHandler)
			log.Println(stus)
		}

		f.Close()
	*/

}
