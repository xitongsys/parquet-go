package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type NameString string
type AgeInt int32

type Student struct {
	Name    NameString  `parquet:"name=name, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Age     AgeInt   `parquet:"name=age, type=INT32, encoding=PLAIN"`
}

func main() {
	var err error
	fw, err := local.NewLocalFileWriter("output/type_alias.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.PageSize = 8 * 1024 //8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   "StudentName",
			Age:    AgeInt(20 + i%5),
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
	fr, err := local.NewLocalFileReader("output/type_alias.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	stus := make([]Student, num)
	if err = pr.Read(&stus); err != nil {
		log.Println("Read error", err)
	}
	log.Println(stus)

	pr.ReadStop()
	fr.Close()
}
