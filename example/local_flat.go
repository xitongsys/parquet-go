package main

import (
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/ParquetWriter"
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

func (self *MyFile) Create(name string) (ParquetFile.ParquetFile, error) {
	file, err := os.Create(name)
	myFile := new(MyFile)
	myFile.File = file
	return myFile, err

}
func (self *MyFile) Open(name string) (ParquetFile.ParquetFile, error) {
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
	var f ParquetFile.ParquetFile
	f = &MyFile{}

	//write flat
	f, _ = f.Create("flat.parquet")
	pw := ParquetWriter.NewParquetWriter(f, new(Student), 4)
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
		pw.Write(stu)
	}
	pw.Flush(true)
	//pw.NameToLower()// convert the field name to lowercase
	pw.WriteStop()
	log.Println("Write Finished")
	f.Close()

	///read flat
	f, _ = f.Open("flat.parquet")
	pr, _ := ParquetReader.NewParquetReader(f, 4)
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		pr.Read(&stus)
		log.Println(stus)
	}

	f.Close()

}
