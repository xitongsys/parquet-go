package main

import (
	"fmt"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/Plugin/JSONWriter"
	"log"
)

func main() {
	md := `{
        "Tag":"name=parquet-go-root",
        "Fields":[
		    {"Tag":"name=Name, type=UTF8, encoding=PLAIN_DICTIONARY"},
		    {"Tag":"name=Age, type=INT32"},
		    {"Tag":"name=Id, type=INT64"},
		    {"Tag":"name=Weight, type=FLOAT"},
		    {"Tag":"name=Sex, type=BOOLEAN"}
        ]
	}`

	//write
	fw, _ := ParquetFile.NewLocalFileWriter("json.parquet")
	pw, _ := JSONWriter.NewJSONWriter(md, fw, 1)

	num := 10
	for i := 0; i < num; i++ {
		rec := `
            {
                "Name":"%s",
                "Age":%d,
                "Id":%d,
                "Weight":%f,
                "Sex":%t
            }
        `

		rec = fmt.Sprintf(rec, "Student Name", 20+i%5, i, 50.0+float32(i)*0.1, i%2 == 0)

		pw.Write(rec)

	}
	pw.Flush(true)
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()

}
