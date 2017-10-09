package main

import (
	"github.com/colinmarc/hdfs"
	. "github.com/xitongsys/parquet-go/ParquetHandler"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"log"
)

type Student struct {
	Name   UTF8
	Age    INT32
	Id     INT64
	Weight FLOAT
	Sex    BOOLEAN
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

func (self *MyFile) Create(name string) (ParquetFile, error) {
	file, err := self.Client.Create(name)
	myFile := new(MyFile)
	myFile.HdfsURL = self.HdfsURL
	myFile.FileWriter = file
	myFile.Client = self.Client
	return myFile, err

}
func (self *MyFile) Open(name string) (ParquetFile, error) {
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
	var f ParquetFile
	myFile := &MyFile{
		HdfsURL: "localhost:9000",
	}
	myFile.Init()
	f = myFile

	//write flat
	f, _ = f.Create("/flat.parquet")
	ph := NewParquetHandler()
	ph.WriteInit(f, new(Student), 4, 30)

	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   UTF8("StudentName"),
			Age:    INT32(20 + i%5),
			Id:     INT64(i),
			Weight: FLOAT(50.0 + float32(i)*0.1),
			Sex:    BOOLEAN(i%2 == 0),
		}
		ph.Write(stu)
	}
	ph.WriteStop()
	log.Println("Write Finished")
	f.Close()

	///read flat
	f, _ = f.Open("/flat.parquet")
	ph = NewParquetHandler()
	rowGroupNum := ph.ReadInit(f, 10)
	for i := 0; i < rowGroupNum; i++ {
		stus := make([]Student, 0)
		ph.ReadOneRowGroupAndUnmarshal(&stus)
		log.Println(stus)
	}

	f.Close()

}
