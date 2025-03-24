package main

import (
	"log"
	"strconv"

	"github.com/hangxie/parquet-go-source/local"
	"github.com/hangxie/parquet-go/reader"
	"github.com/hangxie/parquet-go/types"
	"github.com/hangxie/parquet-go/writer"
)

type TypeList struct {
	Bool              bool    `parquet:"name=bool, type=BOOLEAN"`
	Int32             int32   `parquet:"name=int32, type=INT32"`
	Int64             int64   `parquet:"name=int64, type=INT64"`
	Int96             string  `parquet:"name=int96, type=INT96"`
	Float             float32 `parquet:"name=float, type=FLOAT"`
	Double            float64 `parquet:"name=double, type=DOUBLE"`
	ByteArray         string  `parquet:"name=bytearray, type=BYTE_ARRAY"`
	Enum              string  `parquet:"name=enum, type=BYTE_ARRAY, convertedtype=ENUM"`
	FixedLenByteArray string  `parquet:"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10"`

	Utf8             string `parquet:"name=utf8, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Int_8            int32  `parquet:"name=int_8, type=INT32, convertedtype=INT32, convertedtype=INT_8"`
	Int_16           int32  `parquet:"name=int_16, type=INT32, convertedtype=INT_16"`
	Int_32           int32  `parquet:"name=int_32, type=INT32, convertedtype=INT_32"`
	Int_64           int64  `parquet:"name=int_64, type=INT64, convertedtype=INT_64"`
	Uint_8           int32  `parquet:"name=uint_8, type=INT32, convertedtype=UINT_8"`
	Uint_16          int32  `parquet:"name=uint_16, type=INT32, convertedtype=UINT_16"`
	Uint_32          int32  `parquet:"name=uint_32, type=INT32, convertedtype=UINT_32"`
	Uint_64          int64  `parquet:"name=uint_64, type=INT64, convertedtype=UINT_64"`
	Date             int32  `parquet:"name=date, type=INT32, convertedtype=DATE"`
	Date2            int32  `parquet:"name=date2, type=INT32, convertedtype=DATE, logicaltype=DATE"`
	TimeMillis       int32  `parquet:"name=timemillis, type=INT32, convertedtype=TIME_MILLIS"`
	TimeMillis2      int32  `parquet:"name=timemillis2, type=INT32, logicaltype=TIME, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	TimeMicros       int64  `parquet:"name=timemicros, type=INT64, convertedtype=TIME_MICROS"`
	TimeMicros2      int64  `parquet:"name=timemicros2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
	TimestampMillis  int64  `parquet:"name=timestampmillis, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampMillis2 int64  `parquet:"name=timestampmillis2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	TimestampMicros  int64  `parquet:"name=timestampmicros, type=INT64, convertedtype=TIMESTAMP_MICROS"`
	TimestampMicros2 int64  `parquet:"name=timestampmicros2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
	Interval         string `parquet:"name=interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12"`

	Decimal1 int32  `parquet:"name=decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
	Decimal2 int64  `parquet:"name=decimal2, type=INT64, convertedtype=DECIMAL, scale=2, precision=18"`
	Decimal3 string `parquet:"name=decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12"`
	Decimal4 string `parquet:"name=decimal4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=20"`

	Decimal5 int32 `parquet:"name=decimal5, type=INT32, scale=2, precision=9, logicaltype=DECIMAL, logicaltype.precision=9, logicaltype.scale=2"`

	Map      map[string]int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List     []string         `parquet:"name=list, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Repeated []int32          `parquet:"name=repeated, type=INT32, repetitiontype=REPEATED"`
}

func main() {
	var err error
	// write
	fw, err := local.NewLocalFileWriter("type.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, new(TypeList), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	num := 10
	for i := 0; i < num; i++ {
		tp := TypeList{
			Bool:              bool(i%2 == 0),
			Int32:             int32(i),
			Int64:             int64(i),
			Int96:             types.StrIntToBinary("12345", "LittleEndian", 12, true),
			Float:             float32(float32(i) * 0.5),
			Double:            float64(float64(i) * 0.5),
			ByteArray:         "ByteArray",
			Enum:              "Enum" + strconv.Itoa(i),
			FixedLenByteArray: "HelloWorld",

			Utf8:            "utf8",
			Int_8:           int32(i),
			Int_16:          int32(i),
			Int_32:          int32(i),
			Int_64:          int64(i),
			Uint_8:          int32(i),
			Uint_16:         int32(i),
			Uint_32:         int32(i),
			Uint_64:         int64(i),
			Date:            int32(i),
			TimeMillis:      int32(i),
			TimeMicros:      int64(i),
			TimestampMillis: int64(i),
			TimestampMicros: int64(i),
			Interval:        types.StrIntToBinary("12345", "LittleEndian", 12, false),

			Decimal1: int32(12345),
			Decimal2: int64(12345),
			Decimal3: types.StrIntToBinary("-12345", "BigEndian", 12, true),
			Decimal4: types.StrIntToBinary("12345", "BigEndian", 0, true),

			Map:      map[string]int32{"One": 1, "Two": 2},
			List:     []string{"item1", "item2"},
			Repeated: []int32{1, 2, 3},
		}
		if err = pw.Write(tp); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	log.Println("Write Finished")
	_ = fw.Close()

	///read
	fr, err := local.NewLocalFileReader("type.parquet")
	if err != nil {
		log.Println("Can't create file reader", err)
		return
	}
	pr, err := reader.NewParquetReader(fr, new(TypeList), 10)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		tps := make([]TypeList, 1)
		if err = pr.Read(&tps); err != nil {
			log.Println("Read error", err)
		}
		tps[0].Decimal3 = types.DECIMAL_BYTE_ARRAY_ToString([]byte(tps[0].Decimal3), 10, 2)
		tps[0].Decimal4 = types.DECIMAL_BYTE_ARRAY_ToString([]byte(tps[0].Decimal4), 20, 2)
		log.Println(tps)
	}
	pr.ReadStop()
	_ = fr.Close()
}
