package main

import (
	"context"
	"log"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetWriter"
)

type student struct {
	Name   string  `parquet:"name=name, type=UTF8"`
	Age    int32   `parquet:"name=age, type=INT32"`
	ID     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
	// var err error
	ctx := context.Background()
	bucket := "my-bucket"
	key := "test/foobar.parquet"
	num := 100

	//write
	fw, err := ParquetFile.NewS3FileWriter(ctx, bucket, key)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pw, err := ParquetWriter.NewParquetWriter(fw, new(student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	for i := 0; i < num; i++ {
		stu := student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			ID:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Sex:    bool(i%2 == 0),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop err", err)
	}
	log.Println("Write Finished")
	fw.Close()

	//read
	log.Println("Start Read")
	fr, err := ParquetFile.NewS3FileReader(ctx, bucket, key)
	// fr, err := ParquetFile.NewLocalFileReader("/Users/shsing/Documents/parquet-go/example/foobar.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := ParquetReader.NewParquetReader(fr, new(student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	num = int(pr.GetNumRows())
	for i := 0; i < num/10; i++ {
		if i%2 == 0 {
			pr.SkipRows(10) //skip 10 rows
			continue
		}
		stus := make([]student, 10) //read 10 rows
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}

	pr.ReadStop()
	fr.Close()
}
