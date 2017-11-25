package main

import (
	"github.com/colinmarc/hdfs"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"log"
)

type Student struct {
	Name   ParquetType.UTF8
	Age    ParquetType.INT32
	Id     ParquetType.INT64
	Weight ParquetType.FLOAT
	Sex    ParquetType.BOOLEAN
}

type MyFile struct {
	HdfsURL    string
	Client     *hdfs.Client
	FilePath   string
	FileReader *hdfs.FileReader
	FileWriter *hdfs.FileWriter
}

func (self *MyFile) Init() error {
	var err error
	self.Client, err = hdfs.New(self.HdfsURL)
	return err
}

func (self *MyFile) Create(name string) (ParquetFile.ParquetFile, error) {
	file, err := self.Client.Create(name)
	myFile := new(MyFile)
	myFile.HdfsURL = self.HdfsURL
	myFile.FileWriter = file
	myFile.Client = self.Client
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
	myFile.HdfsURL = self.HdfsURL
	myFile.Client = self.Client
	myFile.FileReader, err = self.Client.Open(name)
	return myFile, err
}
func (self *MyFile) Seek(offset int, pos int) (int64, error) {
	return self.FileReader.Seek(int64(offset), pos)
}

func (self *MyFile) Read(b []byte) (n int, err error) {
	return self.FileReader.Read(b)
}

func (self *MyFile) Write(b []byte) (n int, err error) {
	return self.FileWriter.Write(b)
}

func (self *MyFile) Close() {
	if self.FileReader != nil {
		self.FileReader.Close()
	}
	if self.FileWriter != nil {
		self.FileWriter.Close()
	}
}

func main() {
	var f ParquetFile.ParquetFile
	myFile := &MyFile{
		HdfsURL: "localhost:9000",
	}
	myFile.Init()
	f = myFile

	//write flat
	f, _ = f.Create("/flat.parquet")
	pw, _ := ParquetWriter.NewParquetWriter(f, new(Student), 4)

	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   ParquetType.UTF8("StudentName"),
			Age:    ParquetType.INT32(20 + i%5),
			Id:     ParquetType.INT64(i),
			Weight: ParquetType.FLOAT(50.0 + float32(i)*0.1),
			Sex:    ParquetType.BOOLEAN(i%2 == 0),
		}
		pw.Write(stu)
	}
	pw.Flush(true)
	pw.WriteStop()
	log.Println("Write Finished")
	f.Close()

	///read flat
	f, _ = f.Open("/flat.parquet")
	pr, _ := ParquetReader.NewParquetReader(f, 4)
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		pr.Read(&stus)
		log.Println(stus)
	}

	f.Close()

}
