package main

import (
	"flag"
	"fmt"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/SchemaTool"
)

func main() {
	cmd := flag.String("cmd", "schema", "command")
	fileName := flag.String("file", "", "file name")

	flag.Parse()

	fr, err := ParquetFile.NewLocalFileReader(*fileName)
	if err != nil {
		fmt.Println("Can't open file ", *fileName)
		return
	}

	pr, err := ParquetReader.NewParquetColumnReader(fr, 1)
	if err != nil {
		fmt.Println("Can't create parquet reader ", err)
		return
	}

	if *cmd == "schema" {
		tree := SchemaTool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)
		fmt.Println("----- Go struct -----")
		fmt.Printf("%s\n", tree.OutputStruct())
		fmt.Println("----- Json schema -----")
		fmt.Printf("%s\n", tree.OutputJsonSchema())
	} else {
		fmt.Println("Unknown command")
	}

}
