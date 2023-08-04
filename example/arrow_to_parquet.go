package main

import (
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

func main() {
	fw, err := local.NewLocalFileWriter("arrow.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "float64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
			{Name: "ts_ms", Type: arrow.FixedWidthTypes.Timestamp_ms},
			{Name: "nullable-int32", Type: arrow.PrimitiveTypes.Int32,
				Nullable: true},
		},
		nil,
	)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for idx := range schema.Fields() {
		switch idx {
		case 0:
			b.Field(idx).(*array.Int64Builder).AppendValues(
				[]int64{int64(1), int64(2), int64(3)}, nil,
			)
		case 1:
			b.Field(idx).(*array.Float64Builder).AppendValues(
				[]float64{float64(1.1), float64(1.2), float64(1.3)}, nil,
			)
		case 2:
			b.Field(idx).(*array.StringBuilder).AppendValues(
				[]string{"a", "b", "c"}, nil,
			)
		case 3:
			n := arrow.Timestamp(time.Now().UnixMilli())
			b.Field(idx).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{n, n, n}, nil)
		case 4:
			colBuilder := b.Field(idx).(*array.Int32Builder)
			colBuilder.Append(1)
			colBuilder.AppendNull()
			colBuilder.Append(2)
			colBuilder.AppendNull()
		}
	}
	rec := b.NewRecord()

	w, err := writer.NewArrowWriter(schema, fw, 1)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	if err = w.WriteArrow(rec); err != nil {
		log.Println("WriteArrow error", err)
		return
	}
	if err = w.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()

	fr, err := local.NewLocalFileReader("arrow.parquet")
	if err != nil {
		log.Println("Can't open file for read", err)
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read rows", err)
		return
	}

	table := ""
	for _, row := range res {
		table = table + fmt.Sprintf("%v\n", row)
	}

	log.Printf("Content of table:\n%s", table)
	log.Print("Read Finished")
}
