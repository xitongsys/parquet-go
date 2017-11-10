package main

import (
	. "github.com/xitongsys/parquet-go/ParquetHandler"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"log"
	"os"
)

type TypeList struct {
	Bool              BOOLEAN
	Int32             INT32
	Int64             INT64
	Int96             INT96
	Float             FLOAT
	Double            DOUBLE
	ByteArray         BYTE_ARRAY
	FixedLenByteArray FIXED_LEN_BYTE_ARRAY `Length:"10"`

	Utf8            UTF8
	Int_8           INT_8
	Int_16          INT_16
	Int_32          INT_32
	Int_64          INT_64
	Uint_8          UINT_8
	Uint_16         UINT_16
	Uint_32         UINT_32
	Uint_64         UINT_64
	Date            DATE
	TimeMillis      TIME_MILLIS
	TimeMicros      TIME_MICROS
	TimestampMillis TIMESTAMP_MILLIS
	TimestampMicros TIMESTAMP_MICROS
	Interval        INTERVAL
	Decimal         DECIMAL `BaseType:"BYTE_ARRAY" Scale:"2" Precision:"2"`
}

type MyFile struct {
	FilePath string
	File     *os.File
}

func (self *MyFile) Create(name string) (ParquetFile, error) {
	file, err := os.Create(name)
	myFile := new(MyFile)
	myFile.File = file
	return myFile, err

}
func (self *MyFile) Open(name string) (ParquetFile, error) {
	var (
		err error
	)
	if name == "" {
		name = self.FilePath
	}

	myFile := new(MyFile)
	myFile.FilePath = name
	myFile.File, err = os.Open(name)
	return myFile, err
}
func (self *MyFile) Seek(offset int, pos int) (int64, error) {
	return self.File.Seek(int64(offset), pos)
}

func (self *MyFile) Read(b []byte) (n int, err error) {
	return self.File.Read(b)
}

func (self *MyFile) Write(b []byte) (n int, err error) {
	return self.File.Write(b)
}

func (self *MyFile) Close() {
	self.File.Close()
}

func main() {
	var f ParquetFile
	f = &MyFile{}

	//write flat
	f, _ = f.Create("type.parquet")
	ph := NewParquetHandler()
	ph.WriteInit(f, new(TypeList), 4, 30)

	num := 10
	for i := 0; i < num; i++ {
		tp := TypeList{
			Bool:              BOOLEAN(i%2 == 0),
			Int32:             INT32(i),
			Int64:             INT64(i),
			Int96:             INT96("012345678912"),
			Float:             FLOAT(float32(i) * 0.5),
			Double:            DOUBLE(float64(i) * 0.5),
			ByteArray:         BYTE_ARRAY("ByteArray"),
			FixedLenByteArray: FIXED_LEN_BYTE_ARRAY("HelloWorld"),

			Utf8:            UTF8("utf8"),
			Int_8:           INT_8(i),
			Int_16:          INT_16(i),
			Int_32:          INT_32(i),
			Int_64:          INT_64(i),
			Uint_8:          UINT_8(i),
			Uint_16:         UINT_16(i),
			Uint_32:         UINT_32(i),
			Uint_64:         UINT_64(i),
			Date:            DATE(i),
			TimeMillis:      TIME_MILLIS(i),
			TimeMicros:      TIME_MICROS(i),
			TimestampMillis: TIMESTAMP_MILLIS(i),
			TimestampMicros: TIMESTAMP_MICROS(i),
			Interval:        INTERVAL("012345678912"),
			Decimal:         DECIMAL("12345"),
		}
		ph.Write(tp)
	}
	ph.Flush()
	ph.WriteStop()
	log.Println("Write Finished")
	f.Close()

	///read flat
	f, _ = f.Open("type.parquet")
	ph = NewParquetHandler()
	rowGroupNum := ph.ReadInit(f, 10)
	for i := 0; i < rowGroupNum; i++ {
		tps := make([]TypeList, 0)
		ph.ReadOneRowGroupAndUnmarshal(&tps)
		log.Println(tps)
	}

	f.Close()

}
