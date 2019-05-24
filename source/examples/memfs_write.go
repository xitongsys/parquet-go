package main

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/xitongsys/parquet-go-source/mem"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/xitongsys/parquet-go/parquet"
)

type Student struct {
	Name   string  `parquet:"name=name, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Id     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
	Day    int32   `parquet:"name=day, type=DATE"`
}

func main() {
	// create in-memory ParquetFile with Closer Function
	// NOTE: closer function can be nil, no action will be
	// run when the writer is closed.
	fw, err := mem.NewMemFileWriter("flat.parquet.snappy", func(name string, r io.Reader) error {
		dat, err := ioutil.ReadAll(r)
		if err != nil {
			log.Printf("error reading data: %v", err)
			os.Exit(1)
		}

		// write file to disk
		if err := ioutil.WriteFile(name, dat, 0644); err != nil {
			log.Printf("error writing result file: %v", err)
		}
		return nil
	})

	if err != nil {
		log.Println("Can't create local file", err)
		return
	}
	//write
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			Id:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Sex:    bool(i%2 == 0),
			Day:    int32(time.Now().Unix() / 3600 / 24),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()
	// os.Exit(1)

	///read
	fr, err := local.NewLocalFileReader("flat.parquet.snappy")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}
	pr.ReadStop()
	fr.Close()

	// NOTE: you can access the underlying MemFs using ParquetFile.GetMemFileFs()
	// EXAMPLE: this will delete the file we created from the in-memory file system
	if err := mem.GetMemFileFs().Remove("flat.parquet.snappy"); err != nil {
		log.Printf("error removing file from memfs: %v", err)
		os.Exit(1)
	}

}
