# parquet-go v1.0.0
[![Travis Status for xitongsys/parquet-go](https://travis-ci.org/xitongsys/parquet-go.svg?branch=master&label=linux+build)](https://travis-ci.org/xitongsys/parquet-go)
[![godoc for xitongsys/parquet-go](https://godoc.org/github.com/nathany/looper?status.svg)](http://godoc.org/github.com/xitongsys/parquet-go)


parquet-go is a pure-go implementation of reading and writing the parquet format file. 
* Support Read/Write Nested/Flat Parquet File
* Support all Types in Parquet
* Very simple to use
* High performance

## Required
* git.apache.org/thrift.git/lib/go/thrift
* github.com/golang/snappy

## Install
Add the parquet-go library to your $GOPATH/src:
```sh
go get github.com/xitongsys/parquet-go
```
Look at a few examples in `example/`. 
```sh
go run example/local_flat.go
```

## Types
There are two Types in Parquet: Base Type and Logical Type. The type definitions is in ParquetType.go. The relationship between the parquet type and go type is shown in following table. OPTIONAL variables are declared as pointers.

|Parquet Type|Go Type|Example|
|-|-|-|
|BOOLEAN|bool|`parquet:"name=name, type=BOOLEAN"`|
|INT32|int32|`parquet:"name=name, type=INT32"`|
|INT64|int64|`parquet:"name=name, type=INT64"`|
|INT96|string|`parquet:"name=name, type=INT96"`|
|FLOAT|float32|`parquet:"name=name, type=FLOAT"`|
|DOUBLE|float64|`parquet:"name=name, type=DOUBLE"`|
|BYTE_ARRAY|string|`parquet:"name=name, type=BYTE_ARRAY"`|
|FIXED_LEN_BYTE_ARRAY|string|`parquet:"name=name, type=FIXED_LEN_BYTE_ARRAY, length=10"`|
|UTF8|string|`parquet:"name=name, type=UTF8"`|
|INT_8|int32|`parquet:"name=name, type=INT_8"`|
|INT_16|int32|`parquet:"name=name, type=INT_16"`|
|INT_32|int32|`parquet:"name=name, type=INT_32"`|
|INT_64|int64|`parquet:"name=name, type=INT_64"`|
|UINT_8|uint32|`parquet:"name=name, type=UINT_8"`|
|UINT_16|uint32|`parquet:"name=name, type=UINT_16"`|
|UINT_32|uint32|`parquet:"name=name, type=UINT_32"`|
|UINT_64|uint64|`parquet:"name=name, type=UINT_64"`|
|DATE|int32|`parquet:"name=name, type=DATE"`|
|TIME_MILLIS|int32|`parquet:"name=name, type=TIME_MILLIS"`|
|TIME_MICROS|int64|`parquet:"name=name, type=TIME_MICROS"`|
|TIMESTAMP_MILLIS|int64|`parquet:"name=name, type=TIMESTAMP_MILLIS"`|
|TIMESTAMP_MICROS|int64|`parquet:"name=name, type=TIMESTAMP_MICROS"`|
|INTERVAL|string|`parquet:"name=name, type=INTERVAL"`|
|DECIMAL|string|`parquet:"name=name, type=DECIMAL, scale=2, precision=2"`|
|List|slice|`parquet:"name=name, type=INT64"`|
|Map|map|`parquet:"name=name, type=INT64, keytype=INT64"`|


## Core Data Structure
The core data structure named "Table":
```golang
type Table struct {
	RepetitionType    parquet.FieldRepetitionType
	Type               parquet.Type
	Path               []string
	MaxDefinitionLevel int32
	MaxRepetitionLevel int32

	Values           []interface{}
	DefinitionLevels []int32
	RepetitionLevels []int32
}
```
Values is the column data; RepetitionLevels is the repetition levels of the values; DefinitionLevels is the definition levels of the values.
The architecture of the data struct is following:
```
Table -> Page
Pages -> Chunk
Chunks -> RowGroup
RowGroups -> ParquetFile
```

## Read/Write
read/write a parquet file need a ParquetFile interface implemented
```golang
type ParquetFile interface {
	Seek(offset int, pos int) (int64, error)
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close()
	Open(name string) (ParquetFile, error)
	Create(name string) (ParquetFile, error)
}
```
Using this interface, parquet-go can read/write parquet file on any plantform(local/hdfs/s3...)

The following is a simple example of read/write parquet file on local disk. It can be found in example directory:
```golang
package main
import (
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"log"
	"time"
)
type Student struct {
	Name   string  `parquet:"name=name, type=UTF8"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Id     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
	Day    int32   `parquet:"name=day, type=DATE"`
}
func main() {
	fw, _ := ParquetFile.NewLocalFileWriter("flat.parquet")
	//write flat
	pw, _ := ParquetWriter.NewParquetWriter(fw, new(Student), 4)
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
		pw.Write(stu)
	}
	pw.Flush(true)
	//pw.NameToLower()// convert the field name to lowercase
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()

	///read flat
	fr, _ := ParquetFile.NewLocalFileReader("flat.parquet")
	pr, err := ParquetReader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Failed new reader", err)
	}
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		pr.Read(&stus)
		log.Println(stus)
	}
	pr.ReadStop()
	fr.Close()
}

```

## Read Columns
If you just want to get some columns data, your can use column reader
```golang
///read flat
fr, _ := ParquetFile.NewLocalFileReader("column.parquet")
pr, err := ParquetReader.NewParquetColumnReader(fr, 4)
if err != nil {
	log.Println("Failed new reader", err)
}
num = int(pr.GetNumRows())
names := make([]interface{}, num)
pr.ReadColumnByPath("name", &names)
log.Println(names)

ids := make([]interface{}, num)
pr.ReadColumnByIndex(2, &ids)
log.Println(ids)
pr.ReadStop()
fr.Close()
```

## Parallel
Read/Write initial functions have a parallel parameters np which is the number of goroutines in reading/writing.
```golang
func NewParquetReader(pFile ParquetFile.ParquetFile, obj interface{}, np int64) (*ParquetReader, error)
func NewParquetWriter(pFile ParquetFile.ParquetFile, obj interface{}, np int64) (*ParquetWriter, error)
```

## Plugin
Plugin is used for some special purpose and will be added gradually.
### CSVWriter Plugin
This plugin is used for data format similar with CSV(not nested).
```golang
func main() {
	md := []CSVWriter.MetadataType{
		{Type: "UTF8", Name: "Name"},
		{Type: "INT32", Name: "Age"},
		{Type: "INT64", Name: "Id"},
		{Type: "FLOAT", Name: "Weight"},
		{Type: "BOOLEAN", Name: "Sex"},
	}
	//write flat
	fw, _ := ParquetFile.NewLocalFileWriter("csv.parquet")
	pw, _ := CSVWriter.NewCSVWriter(md, fw, 1)

	num := 10
	for i := 0; i < num; i++ {
		data := []string{
			fmt.Sprintf("%s_%d", "Student Name", i),
			fmt.Sprintf("%d", 20+i%5),
			fmt.Sprintf("%d", i),
			fmt.Sprintf("%f", 50.0+float32(i)*0.1),
			fmt.Sprintf("%t", i%2 == 0),
		}
		rec := make([]*string, len(data))
		for j := 0; j < len(data); j++ {
			rec[j] = &data[j]
		}
		pw.WriteString(rec)

		data2 := []interface{}{
			ParquetType.UTF8("Student Name"),
			ParquetType.INT32(20 + i*5),
			ParquetType.INT64(i),
			ParquetType.FLOAT(50.0 + float32(i)*0.1),
			ParquetType.BOOLEAN(i%2 == 0),
		}
		pw.Write(data2)
	}
	pw.Flush(true)
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()
}
```

## Performance
A very simple performance test of writing/reading parquet did on Linux host (JRE 1.8.0, Golang 1.7.5, 23GB, 24 Cores). It is faster than java :)

Write Test Results  
![](https://github.com/xitongsys/parquet-go/blob/master/example/WriteRes.png)

Read Test Results  
![](https://github.com/xitongsys/parquet-go/blob/master/example/ReadRes.png)


