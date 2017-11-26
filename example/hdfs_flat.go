package main

import (
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

func main() {
	//write flat
	fw, _ := ParquetFile.NewHdfsFileWriter([]string{"localhost:9000"}, "root", "/flat.parquet")
	pw, _ := ParquetWriter.NewParquetWriter(fw, new(Student), 4)

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
	fw.Close()

	///read flat
	fr, _ := ParquetFile.NewHdfsFileReader([]string{"localhost:9000"}, "", "/flat.parquet")
	pr, _ := ParquetReader.NewParquetReader(fr, 4)
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		pr.Read(&stus)
		log.Println(stus)
	}
	fr.Close()
}
