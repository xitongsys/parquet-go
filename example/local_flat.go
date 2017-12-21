package main

import (
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"log"
	"time"
)

type Student struct {
	Name   string  `parquet:"name=name, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Id     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
	Day    int32   `parquet:"name=day, type=DATE"`
}

func main() {
	fw, _ := ParquetFile.NewLocalFileWriter("flat.parquet")

	//write flat
	pw, _ := ParquetWriter.NewParquetWriter(fw, new(Student), 10)
	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			Id:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Sex:    bool(i%2 == 0),
			Day:    int32(time.Now().Unix() / 3600 / 24),
		}
		pw.Write(stu)
	}
	pw.Flush(true)
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()

	///read flat
	fr, _ := ParquetFile.NewLocalFileReader("flat.parquet")
	pr, err := ParquetReader.NewParquetReader(fr, new(Student), 1)
	if err != nil {
		log.Println("Failed new reader", err)
	}
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		pr.Read(&stus)
		log.Println(stus)
	}

	pr.ReadStop()
	fr.Close()

}
