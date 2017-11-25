package main

import (
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"log"
	"os"
)

type TypeList struct {
	Bool              ParquetType.BOOLEAN
	Int32             ParquetType.INT32
	Int64             ParquetType.INT64
	Int96             ParquetType.INT96
	Float             ParquetType.FLOAT
	Double            ParquetType.DOUBLE
	ByteArray         ParquetType.BYTE_ARRAY
	FixedLenByteArray ParquetType.FIXED_LEN_BYTE_ARRAY `Length:"10"`

	Utf8            ParquetType.UTF8
	Int_8           ParquetType.INT_8
	Int_16          ParquetType.INT_16
	Int_32          ParquetType.INT_32
	Int_64          ParquetType.INT_64
	Uint_8          ParquetType.UINT_8
	Uint_16         ParquetType.UINT_16
	Uint_32         ParquetType.UINT_32
	Uint_64         ParquetType.UINT_64
	Date            ParquetType.DATE
	TimeMillis      ParquetType.TIME_MILLIS
	TimeMicros      ParquetType.TIME_MICROS
	TimestampMillis ParquetType.TIMESTAMP_MILLIS
	TimestampMicros ParquetType.TIMESTAMP_MICROS
	Interval        ParquetType.INTERVAL
	Decimal         ParquetType.DECIMAL `BaseType:"BYTE_ARRAY" Scale:"2" Precision:"2"`
}

type MyFile struct {
	FilePath string
	File     *os.File
}

func (self *MyFile) Create(name string) (ParquetFile.ParquetFile, error) {
	file, err := os.Create(name)
	myFile := new(MyFile)
	myFile.File = file
	return myFile, err

}
func (self *MyFile) Open(name string) (ParquetFile.ParquetFile, error) {
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
	var f ParquetFile.ParquetFile
	f = &MyFile{}

	//write flat
	f, _ = f.Create("type.parquet")
	pw := ParquetWriter.NewParquetWriter(f, new(TypeList), 4)
	num := 10
	for i := 0; i < num; i++ {
		tp := TypeList{
			Bool:              ParquetType.BOOLEAN(i%2 == 0),
			Int32:             ParquetType.INT32(i),
			Int64:             ParquetType.INT64(i),
			Int96:             ParquetType.INT96("012345678912"),
			Float:             ParquetType.FLOAT(float32(i) * 0.5),
			Double:            ParquetType.DOUBLE(float64(i) * 0.5),
			ByteArray:         ParquetType.BYTE_ARRAY("ByteArray"),
			FixedLenByteArray: ParquetType.FIXED_LEN_BYTE_ARRAY("HelloWorld"),

			Utf8:            ParquetType.UTF8("utf8"),
			Int_8:           ParquetType.INT_8(i),
			Int_16:          ParquetType.INT_16(i),
			Int_32:          ParquetType.INT_32(i),
			Int_64:          ParquetType.INT_64(i),
			Uint_8:          ParquetType.UINT_8(i),
			Uint_16:         ParquetType.UINT_16(i),
			Uint_32:         ParquetType.UINT_32(i),
			Uint_64:         ParquetType.UINT_64(i),
			Date:            ParquetType.DATE(i),
			TimeMillis:      ParquetType.TIME_MILLIS(i),
			TimeMicros:      ParquetType.TIME_MICROS(i),
			TimestampMillis: ParquetType.TIMESTAMP_MILLIS(i),
			TimestampMicros: ParquetType.TIMESTAMP_MICROS(i),
			Interval:        ParquetType.INTERVAL("012345678912"),
			Decimal:         ParquetType.DECIMAL("12345"),
		}
		pw.Write(tp)
	}
	pw.Flush(true)
	pw.WriteStop()
	log.Println("Write Finished")
	f.Close()

	///read flat
	f, _ = f.Open("type.parquet")
	pr, _ := ParquetReader.NewParquetReader(f, 10)
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		tps := make([]TypeList, 1)
		pr.Read(&tps)
		log.Println(tps)
	}

	f.Close()

}
