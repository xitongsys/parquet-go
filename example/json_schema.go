package main

import (
	"log"
	"time"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
)

type Student struct {
	Name   string
	Age    int32
	Id     int64
	Weight float32
	Sex    bool
	Day    int32
}

var jsonSchema string = `{
    "Tag":"name=parquet-go-root",
    "Fields":[
        {"Tag":"name=name, inname=Name, type=UTF8, encoding=PLAIN_DICTIONARY"},
        {"Tag":"name=age, inname=Age, type=INT32"},
        {"Tag":"name=id, inname=Id, type=INT64"},
        {"Tag":"name=weight, inname=Weight, type=FLOAT"},
        {"Tag":"name=sex, inname=Sex, type=BOOLEAN"},
        {"Tag":"name=day, inname=Day, type=DATE"}
    ]
}
`

func main() {
	var err error
	fw, err := ParquetFile.NewLocalFileWriter("json_schema.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := ParquetWriter.NewParquetWriter(fw, nil, 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	if err = pw.SetSchemaHandlerFromJSON(jsonSchema); err != nil {
		log.Println("Can't set schema from json ", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
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
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()

	///read
	fr, err := ParquetFile.NewLocalFileReader("json_schema.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := ParquetReader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	if err = pr.SetSchemaHandlerFromJSON(jsonSchema); err != nil {
		log.Println("Can't set schema from json", err)
		return
	}

	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}

	pr.ReadStop()
	fr.Close()

}
