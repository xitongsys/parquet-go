package main

import (
	"fmt"
	"os"
	//	"reflect"
	. "Reader"
)

func ReadParquet(fname string) {
	file, _ := os.Open(fname)
	defer file.Close()

	res := Reader(file)
	/*
		for _, v := range res {
			fmt.Println(v.Path)
			for i, v2 := range v.Values {
				if reflect.TypeOf(v2) == reflect.TypeOf([]uint8{}) {
					fmt.Print(string(v2.([]byte)))
				} else {
					fmt.Print(v2)
				}
				fmt.Printf(" %d %d\n", v.DefinitionLevels[i], v.RepetitionLevels[i])
			}
		}
	*/

	for _, rowGroup := range res {
		for _, chunk := range rowGroup.Chunks {
			for _, page := range chunk.Pages {
				fmt.Println(page.DataTable.Path)
				for i := 0; i < len(page.DataTable.Values); i++ {
					fmt.Println(page.DataTable.Values[i], page.DataTable.RepetitionLevels[i], page.DataTable.DefinitionLevels[i])
				}
			}
		}
	}
}

func main() {
	ReadParquet("./class.snappy.parquet")
}
