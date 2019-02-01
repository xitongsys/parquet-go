package main

import (
	"flag"
	"fmt"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/SchemaTool"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/SizeTool"
)

func main() {
	cmd := flag.String("cmd", "schema", "command to run. Allowed values: schema, rowcount or size")
	fileName := flag.String("file", "", "file name")
	withTags := flag.Bool("tag", false, "show struct tags")
	withPrettySize := flag.Bool("pretty", false, "show pretty size")
	uncompressedSize := flag.Bool("uncompressed", false, "show uncompressed size")

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
	case "size":
		fmt.Println(SizeTool.GetParquetFileSize(*fileName, pr, *withPrettySize, *uncompressedSize))
	default:
		fmt.Println("Unknown command")
	}

}
