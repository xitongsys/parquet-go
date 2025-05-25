package main

import (
	"fmt"
	"os"

	"github.com/hangxie/parquet-go/v2/reader"

	"github.com/hangxie/parquet-go/v2/source/http"
)

func main() {
	httpReader, err := http.NewHttpReader(
		"https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet",
		false,
		false,
		map[string]string{},
	)
	if err != nil {
		fmt.Println("failed to create HTTP reader:", err.Error())
		os.Exit(1)
	}
	parquetReader, err := reader.NewParquetReader(httpReader, nil, 4)
	if err != nil {
		fmt.Println("failed to create Parquet reader:", err.Error())
		os.Exit(1)
	}
	fmt.Println("Row count:", parquetReader.GetNumRows())
}
