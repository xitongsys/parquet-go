package main

import (
	"fmt"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/Plugin/CSVWriter"
	"log"
)

func main() {
	md := []CSVWriter.MetadataType{
		{Type: "UTF8", Name: "Name"},
		{Type: "INT32", Name: "Age"},
		{Type: "INT64", Name: "Id"},
		{Type: "FLOAT", Name: "Weight"},
		{Type: "BOOLEAN", Name: "Sex"},
	}

	//write flat
	fw, _ := ParquetFile.NewLocalFileWriter("csv.parquet")
	pw, _ := CSVWriter.NewCSVWriter(md, fw, 4)

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
		pw.WriteString(rec)

		data2 := []interface{}{
			ParquetType.UTF8("Student Name"),
			ParquetType.INT32(20 + i*5),
			ParquetType.INT64(i),
			ParquetType.FLOAT(50.0 + float32(i)*0.1),
			ParquetType.BOOLEAN(i%2 == 0),
		}
		pw.Write(data2)
	}
	pw.Flush(true)
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()

}
