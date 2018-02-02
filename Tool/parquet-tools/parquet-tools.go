package main

import (
	"log"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/Tool/parquet-tools/SchemaOutput"
)

func main() {

	fr, err := ParquetFile.NewLocalFileReader("flat.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := ParquetReader.NewParquetColumnReader(fr, 1)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	tree := SchemaOutput.CreateTree(pr.SchemaHandler.SchemaElements)
	log.Println(tree)
	log.Println(tree.OutputStruct())
	log.Println(tree.OutputJsonSchema())
}
