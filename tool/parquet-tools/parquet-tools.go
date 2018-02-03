package main

import (
	"log"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/SchemaTool"
)

func main() {

	fr, err := ParquetFile.NewLocalFileReader("/home/zxt/b.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := ParquetReader.NewParquetColumnReader(fr, 1)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	tree := SchemaTool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)
	log.Println(tree)
	log.Println("\n", tree.OutputStruct())
	log.Println(tree.OutputJsonSchema())
}
