package main

import (
	"log"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type Student struct {
	Name    string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age     int32   `parquet:"name=age, type=INT32, encoding=PLAIN"`
	Id      int64   `parquet:"name=id, type=INT64"`
	Weight  float32 `parquet:"name=weight, type=FLOAT"`
	Sex     bool    `parquet:"name=sex, type=BOOLEAN"`
	Day     int32   `parquet:"name=day, type=INT32, convertedtype=DATE"`
	Ignored int32   // without parquet tag and won't write
}

func main() {
	var err error
	fw, err := local.NewLocalFileWriter("keyvalue.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	// write
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8K
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

	// To add KeyValueMetadata, you must call the Flush after all data written
	_ = pw.Flush(true)

	// add global KeyValueMetadata
	pw.Footer.KeyValueMetadata = make([]*parquet.KeyValue, 0)
	keyValueGlobal := parquet.NewKeyValue()
	valueGlobal := "valueGlobal"
	keyValueGlobal.Key, keyValueGlobal.Value = "keyGlobal", &valueGlobal

	// see column information
	// log.Println(pw.SchemaHandler.MapIndex)

	// add KeyValueMetadata in ColumnChunk
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, column := range rowGroup.Columns {
			pathInSchema := column.MetaData.PathInSchema
			ln := len(pathInSchema)
			if pathInSchema[ln-1] == "Weight" {
				key, value := "unit", "kg"
				keyValue := parquet.NewKeyValue()
				keyValue.Key, keyValue.Value = key, &value

				column.MetaData.KeyValueMetadata = []*parquet.KeyValue{
					keyValue,
				}
			}
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	_ = fw.Close()

	///read
	fr, err := local.NewLocalFileReader("keyvalue.parquet")
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
	_ = fr.Close()
}
