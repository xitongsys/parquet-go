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
	fmt.Println(res)
}

func main() {
	ReadParquet("./class.snappy.parquet")
}
