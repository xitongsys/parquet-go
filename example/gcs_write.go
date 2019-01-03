package main

import (
	"context"
	"fmt"
	"log"

	"github.com/nauto/parquet-go/ParquetFile"
	"github.com/nauto/parquet-go/ParquetType"
	"github.com/nauto/parquet-go/ParquetWriter"
)

func main() {
	var err error
	md := []string{
		"name=Name, type=UTF8, encoding=PLAIN_DICTIONARY",
		"name=Age, type=INT32",
		"name=Id, type=INT64",
		"name=Weight, type=FLOAT",
		"name=Sex, type=BOOLEAN",
	}
	ctx := context.Background()
	projectId := "Please change this to your own gCloud project Id"
	bucketName := "Your bucket name"
	fileName := "gcs_example/csv.parquet"

	//write
	fw, err := ParquetFile.NewGcsFileWriter(ctx, projectId, bucketName, fileName)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pw, err := ParquetWriter.NewCSVWriter(md, fw, 4)
	if err != nil {
		log.Println("Can't create csv writer", err)
		return
	}

	num := 10
	for i := 0; i < num; i++ {
		data := []string{
			fmt.Sprintf("%s_%d", "Student Name", i),
			fmt.Sprintf("%d", 20+i%5),
			fmt.Sprintf("%d", i),
			fmt.Sprintf("%f", 50.0+float32(i)*0.1),
			fmt.Sprintf("%t", i%2 == 0),
		}
		rec := make([]*string, len(data))
		for j := 0; j < len(data); j++ {
			rec[j] = &data[j]
		}
		if err = pw.WriteString(rec); err != nil {
			log.Println("WriteString error", err)
		}

		data2 := []interface{}{
			ParquetType.BYTE_ARRAY("Student Name"),
			ParquetType.INT32(20 + i%5),
			ParquetType.INT64(i),
			ParquetType.FLOAT(50.0 + float32(i)*0.1),
			ParquetType.BOOLEAN(i%2 == 0),
		}
		if err = pw.Write(data2); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	log.Println("Write Finished")
	fw.Close()

}
