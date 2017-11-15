package main

import (
	"github.com/xitongsys/parquet-go/ParquetHandler"
	"github.com/xitongsys/parquet-go/ParquetType"
	"log"
	"os"
	"time"
)

type Student struct {
	Name   ParquetType.UTF8
	Age    ParquetType.INT32
	Id     ParquetType.INT64
	Weight ParquetType.FLOAT
	Sex    ParquetType.BOOLEAN
	Day    ParquetType.DATE
}

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

func main() {
	var f ParquetHandler.ParquetFile
	f = &MyFile{}

	//write flat
	f, _ = f.Create("flat.parquet")
	ph := ParquetHandler.NewParquetHandler()
	ph.WriteInit(f, new(Student), 4, 30)

	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   ParquetType.UTF8("StudentName"),
			Age:    ParquetType.INT32(20 + i%5),
			Id:     ParquetType.INT64(i),
			Weight: ParquetType.FLOAT(50.0 + float32(i)*0.1),
			Sex:    ParquetType.BOOLEAN(i%2 == 0),
			Day:    ParquetType.DATE(time.Now().Unix() / 3600 / 24),
		}
		ph.Write(stu)
	}
	ph.Flush()
	//ph.NameToLower()// convert the field name to lowercase
	ph.WriteStop()
	log.Println("Write Finished")
	f.Close()

	///read flat
	f, _ = f.Open("flat.parquet")
	ph = ParquetHandler.NewParquetHandler()
	rowGroupNum := ph.ReadInit(f, 10)
	for i := 0; i < rowGroupNum; i++ {
		stus := make([]Student, 0)
		ph.ReadOneRowGroupAndUnmarshal(&stus)
		log.Println(stus)
	}

	f.Close()

}
