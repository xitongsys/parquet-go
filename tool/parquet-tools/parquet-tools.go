package main

import (
	"flag"
	"fmt"

	"github.com/nauto/parquet-go/ParquetFile"
	"github.com/nauto/parquet-go/ParquetReader"
	"github.com/nauto/parquet-go/tool/parquet-tools/SchemaTool"
)

func main() {
	cmd := flag.String("cmd", "schema", "command")
	fileName := flag.String("file", "", "file name")
	withTags := flag.Bool("tag", false, "show struct tags")

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

	switch *cmd {
	case "schema":
		tree := SchemaTool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)
		fmt.Println("----- Go struct -----")
		fmt.Printf("%s\n", tree.OutputStruct(*withTags))
		fmt.Println("----- Json schema -----")
		fmt.Printf("%s\n", tree.OutputJsonSchema())
	case "rowcount":
		fmt.Println(pr.GetNumRows())
	default:
		fmt.Println("Unknown command")
	}

}
