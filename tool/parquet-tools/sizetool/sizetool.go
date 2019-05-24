package sizetool

import (
	"fmt"

	"github.com/xitongsys/parquet-go/reader"
)

func GetParquetFileSize(name string, pr *reader.ParquetReader, pretty, uncompressedSize bool) string {
	var size int64
	if uncompressedSize {
		size = getUncompressedSize(pr)
	} else {
		size = getCompressedSize(pr)
	}

	if pretty {
		return fmt.Sprintf("%s: %s", name, getPrettySize(size))
	}
	return fmt.Sprintf("%s: %d bytes", name, size)
}

func getUncompressedSize(pr *reader.ParquetReader) int64 {
	var size int64
	for _, rg := range pr.Footer.RowGroups {
		size += rg.TotalByteSize
	}
	return size
}

func getCompressedSize(pr *reader.ParquetReader) int64 {
	var size int64
	for _, rg := range pr.Footer.RowGroups {
		for _, cc := range rg.Columns {
			size += cc.MetaData.TotalCompressedSize
		}
	}
	return size
}

func getPrettySize(size int64) string {
	const (
		oneKB = 1 << 10
		oneMB = 1 << 20
		oneGB = 1 << 30
		oneTB = 1 << 40
		onePB = 1 << 50
	)
	if size/oneKB < 1 {
		return fmt.Sprintf("%d bytes", size)
	}
	if size/oneMB < 1 {
		return fmt.Sprintf("%.3f KB", float64(size)/float64(oneKB))
	}
	if size/oneGB < 1 {
		return fmt.Sprintf("%.3f MB", float64(size)/float64(oneMB))
	}
	if size/oneTB < 1 {
		return fmt.Sprintf("%.3f GB", float64(size)/float64(oneGB))
	}
	if size/onePB < 1 {
		return fmt.Sprintf("%.3f TB", float64(size)/float64(oneTB))
	}
	return fmt.Sprintf("%.3f PB", float64(size)/float64(onePB))
}
