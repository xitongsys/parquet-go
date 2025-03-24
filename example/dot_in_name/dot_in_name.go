package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type A struct {
	V1 int32 `parquet:"name=b.c, type=INT32, encoding=PLAIN"`
	V2 B     `parquet:"name=b"`
	V3 int32 `parquet:"name=c, type=INT32, encoding=PLAIN"`
}

type B struct {
	C int32 `parquet:"name=c, type=INT32, encoding=PLAIN"`
}

func main() {
	var err error
	fw, err := local.NewLocalFileWriter("a.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	// write
	pw, err := writer.NewParquetWriter(fw, new(A), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 10
	for i := 0; i < num; i++ {
		o := A{
			V1: 1,
			V2: B{
				C: 2,
			},
			V3: 3,
		}
		if err = pw.Write(o); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	_ = fw.Close()

	///read all
	fr, err := local.NewLocalFileReader("a.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, new(A), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	os := make([]A, num)

	if err = pr.Read(&os); err != nil {
		log.Println("Read error", err)
	}
	log.Println(os)

	pr.ReadStop()
	_ = fr.Close()

	///read column by path
	fr, err = local.NewLocalFileReader("a.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err = reader.NewParquetReader(fr, new(A), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	cn := pr.GetNumRows()
	v1, _, _, _ := pr.ReadColumnByPath("parquet_go_root\x01b.c", cn)
	v2, _, _, _ := pr.ReadColumnByPath("parquet_go_root\x01b\x01c", cn)
	v3, _, _, _ := pr.ReadColumnByPath("parquet_go_root\x01c", cn)
	log.Println(v1, v2, v3)

	pr.ReadStop()
	_ = fr.Close()
}
