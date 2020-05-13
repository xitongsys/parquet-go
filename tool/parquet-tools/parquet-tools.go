package main

import (
	"flag"
	"fmt"
	"encoding/json"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/sizetool"
)

func main() {
	cmd := flag.String("cmd", "schema", "command to run. Allowed values: schema, rowcount, size, cat")
	fileName := flag.String("file", "", "file name")
	withTags := flag.Bool("tag", false, "show struct tags")
	withPrettySize := flag.Bool("pretty", false, "show pretty size")
	uncompressedSize := flag.Bool("uncompressed", false, "show uncompressed size")
	catCount := flag.Int("count", 1000, "max count to cat. If it is nil, only show first 1000 records.")

	flag.Parse()

	fr, err := local.NewLocalFileReader(*fileName)
	if err != nil {
		fmt.Println("Can't open file ", *fileName)
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		fmt.Println("Can't create parquet reader ", err)
		return
	}

	switch *cmd {
	case "schema":
		tree := schematool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)
		fmt.Println("----- Go struct -----")
		fmt.Printf("%s\n", tree.OutputStruct(*withTags))
		fmt.Println("----- Json schema -----")
		fmt.Printf("%s\n", tree.OutputJsonSchema())
	case "rowcount":
		fmt.Println(pr.GetNumRows())
	case "size":
		fmt.Println(sizetool.GetParquetFileSize(*fileName, pr, *withPrettySize, *uncompressedSize))
	case "cat":
		totCnt := 0
		for totCnt < *catCount {
			cnt := *catCount - totCnt
			if cnt > 1000 {
				cnt = 1000
			}

			res, err := pr.ReadByNumber(cnt)
			if err != nil {
				fmt.Println("Can't cat ", err)
				return
			}
	
			jsonBs, err := json.Marshal(res)
			if err != nil {
				fmt.Println("Can't to json ", err)
				return
			}
	
			fmt.Println(string(jsonBs))

			totCnt += cnt
		}
		

	default:
		fmt.Println("Unknown command")
	}

}
