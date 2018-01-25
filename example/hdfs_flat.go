package main

import (
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"log"
)

type Student struct {
	Name   string  `parquet:"name=name, type=UTF8"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Id     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
	//write
	fw, _ := ParquetFile.NewHdfsFileWriter([]string{"localhost:9000"}, "root", "/flat.parquet")
	pw, _ := ParquetWriter.NewParquetWriter(fw, new(Student), 4)

	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			Id:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Sex:    bool(i%2 == 0),
		}
		pw.Write(stu)
	}
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()

	///read
	fr, _ := ParquetFile.NewHdfsFileReader([]string{"localhost:9000"}, "", "/flat.parquet")
	pr, _ := ParquetReader.NewParquetReader(fr, new(Student), 4)
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		pr.Read(&stus)
		log.Println(stus)
	}
	pr.ReadStop()
	fr.Close()
}
