package main

import (
	"fmt"
	"os"
	//	"reflect"
	. "Reader"
	"parquet"
)

func Read(fname string) {
	file, _ := os.Open(fname)
	defer file.Close()

	res := ReadParquet(file)
	for _, rowGroup := range res {
		for _, chunk := range rowGroup.Chunks {
			for _, page := range chunk.Pages {
				fmt.Println(page.DataTable.Path)
				for i := 0; i < len(page.DataTable.Values); i++ {
					if page.Header.GetType() == parquet.PageType_DATA_PAGE {
						fmt.Println(page.DataTable.Values[i],
							page.DataTable.RepetitionLevels[i],
							page.DataTable.DefinitionLevels[i])
					}
				}
			}
		}
	}
}

func main() {
	Read("./class.snappy.parquet")
}
