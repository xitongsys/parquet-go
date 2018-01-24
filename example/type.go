package main

import (
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"log"
)

type TypeList struct {
	Bool              bool    `parquet:"name=bool, type=BOOLEAN"`
	Int32             int32   `parquet:"name=int32, type=INT32"`
	Int64             int64   `parquet:"name=int64, type=INT64"`
	Int96             string  `parquet:"name=int96, type=INT96"`
	Float             float32 `parquet:"name=float, type=FLOAT"`
	Double            float64 `parquet:"name=double, type=DOUBLE"`
	ByteArray         string  `parquet:"name=bytearray, type=BYTE_ARRAY"`
	FixedLenByteArray string  `parquet:"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10"`

	Utf8            string `parquet:"name=utf8, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Int_8           int32  `parquet:"name=int_8, type=INT_8"`
	Int_16          int32  `parquet:"name=int_16, type=INT_16"`
	Int_32          int32  `parquet:"name=int_32, type=INT_32"`
	Int_64          int64  `parquet:"name=int_64, type=INT_64"`
	Uint_8          uint32 `parquet:"name=uint_8, type=UINT_8"`
	Uint_16         uint32 `parquet:"name=uint_16, type=UINT_16"`
	Uint_32         uint32 `parquet:"name=uint_32, type=UINT_32"`
	Uint_64         uint64 `parquet:"name=uint_64, type=UINT_64"`
	Date            int32  `parquet:"name=date, type=DATE"`
	TimeMillis      int32  `parquet:"name=timemillis, type=TIME_MILLIS"`
	TimeMicros      int64  `parquet:"name=timemicros, type=TIME_MICROS"`
	TimestampMillis int64  `parquet:"name=timestampmillis, type=TIMESTAMP_MILLIS"`
	TimestampMicros int64  `parquet:"name=timestampmicros, type=TIMESTAMP_MICROS"`
	Interval        string `parquet:"name=interval, type=INTERVAL"`

	Decimal1 int32  `parquet:"name=decimal1, type=DECIMAL, scale=2, precision=9, basetype=INT32"`
	Decimal2 int64  `parquet:"name=decimal2, type=DECIMAL, scale=2, precision=18, basetype=INT64"`
	Decimal3 string `parquet:"name=decimal3, type=DECIMAL, scale=2, precision=10, basetype=FIXED_LEN_BYTE_ARRAY, length=12"`
	Decimal4 string `parquet:"name=decimal4, type=DECIMAL, scale=2, precision=20, basetype=BYTE_ARRAY"`

	Map      map[string]int32 `parquet:"name=map, type=MAP, keytype=UTF8, valuetype=INT32"`
	List     []string         `parquet:"name=list, type=LIST, valuetype=UTF8"`
	Repeated []int32          `parquet:"name=repeated, type=INT32, repetitiontype=REPEATED"`
}

func main() {
	//write
	fw, _ := ParquetFile.NewLocalFileWriter("type.parquet")
	pw, _ := ParquetWriter.NewParquetWriter(fw, new(TypeList), 4)
	num := 10
	for i := 0; i < num; i++ {
		tp := TypeList{
			Bool:              bool(i%2 == 0),
			Int32:             int32(i),
			Int64:             int64(i),
			Int96:             ParquetType.StrIntToBinary("12345", "LittleEndian", 12, true),
			Float:             float32(float32(i) * 0.5),
			Double:            float64(float64(i) * 0.5),
			ByteArray:         "ByteArray",
			FixedLenByteArray: "HelloWorld",

			Utf8:            "utf8",
			Int_8:           int32(i),
			Int_16:          int32(i),
			Int_32:          int32(i),
			Int_64:          int64(i),
			Uint_8:          uint32(i),
			Uint_16:         uint32(i),
			Uint_32:         uint32(i),
			Uint_64:         uint64(i),
			Date:            int32(i),
			TimeMillis:      int32(i),
			TimeMicros:      int64(i),
			TimestampMillis: int64(i),
			TimestampMicros: int64(i),
			Interval:        ParquetType.StrIntToBinary("12345", "LittleEndian", 12, false),

			Decimal1: int32(12345),
			Decimal2: int64(12345),
			Decimal3: ParquetType.StrIntToBinary("-12345", "BigEndian", 12, true),
			Decimal4: ParquetType.StrIntToBinary("12345", "BigEndian", 0, true),

			Map:      map[string]int32{"One": 1, "Two": 2},
			List:     []string{"item1", "item2"},
			Repeated: []int32{1, 2, 3},
		}
		pw.Write(tp)
	}
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()

	///read
	fr, _ := ParquetFile.NewLocalFileReader("type.parquet")
	pr, _ := ParquetReader.NewParquetReader(fr, new(TypeList), 10)
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		tps := make([]TypeList, 1)
		pr.Read(&tps)
		log.Println(tps)
	}
	pr.ReadStop()
	fr.Close()

}
