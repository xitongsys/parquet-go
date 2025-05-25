package main

import (
	"log"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

type (
	NameString string
	AgeInt     int32
)

type Student struct {
	Name NameString `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age  AgeInt     `parquet:"name=age, type=INT32, encoding=PLAIN"`
}

func main() {
	var err error
	fw, err := local.NewLocalFileWriter("type_alias.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	// write
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name: "StudentName",
			Age:  AgeInt(20 + i%5),
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
	_ = fw.Close()

	///read
	fr, err := local.NewLocalFileReader("type_alias.parquet")
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
	_ = fr.Close()
}
