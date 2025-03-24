package main

import (
	"fmt"
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type DateItem struct {
	RequiredDate int32  `parquet:"name=requiredDate, type=INT32, convertedtype=DATE"`
	OptionalDate *int32 `parquet:"name=optionalDate, type=INT32, convertedtype=DATE, repetitiontype=OPTIONAL"`
	NullDate     *int32 `parquet:"name=nullDate, type=INT32, convertedtype=DATE, repetitiontype=OPTIONAL"`
}

func main() {
	var err error

	outputFile := "date.parquet"
	fw, err := local.NewLocalFileWriter(outputFile)
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, new(DateItem), 2)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	optionalDate := int32(19619)

	item := DateItem{
		RequiredDate: 19618,
		NullDate:     nil,
		OptionalDate: &optionalDate,
	}
	if err = pw.Write(item); err != nil {
		log.Printf("Write error %s\n", err)
	}

	if err = pw.WriteStop(); err != nil {
		log.Printf("WriteStop error %s\n", err)
		return
	}

	log.Println("Write Finished")
	_ = fw.Close()

	///read
	fr, err := local.NewLocalFileReader("date.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, new(DateItem), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num := int(pr.GetNumRows())
	dateItem := make([]DateItem, num)
	if err = pr.Read(&dateItem); err != nil {
		log.Println("Read error", err)
	}
	fmt.Printf("RequiredDate: %v\n", dateItem[0].RequiredDate)
	fmt.Printf("OptionalDate %v\n", *dateItem[0].OptionalDate)
	fmt.Printf("NullDate: %v\n", dateItem[0].NullDate)

	pr.ReadStop()
	_ = fr.Close()
}
