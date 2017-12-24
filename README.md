# parquet-go v1.1.0
[![Travis Status for xitongsys/parquet-go](https://travis-ci.org/xitongsys/parquet-go.svg?branch=master&label=linux+build)](https://travis-ci.org/xitongsys/parquet-go)
[![godoc for xitongsys/parquet-go](https://godoc.org/github.com/nathany/looper?status.svg)](http://godoc.org/github.com/xitongsys/parquet-go)


parquet-go is a pure-go implementation of reading and writing the parquet format file. 
* Support Read/Write Nested/Flat Parquet File
* Simple to use
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
There are two types in Parquet: Primitive Type and Logical Type. Logical types are stored as primitive types. The following list is the currently implemented data types:

|Parquet Type|Primitive Type|Go Type|
|-|-|-|
|BOOLEAN|BOOLEAN|bool|
|INT32|INT32|int32|
|INT64|INT64|int64|
|INT96|INT96|string|
|FLOAT|FLOAT|float32|
|DOUBLE|DOUBLE|float64|
|BYTE_ARRAY|BYTE_ARRAY|string|
|FIXED_LEN_BYTE_ARRAY|FIXED_LEN_BYTE_ARRAY|string|
|UTF8|BYTE_ARRAY|string|
|INT_8|INT32|int32|
|INT_16|INT32|int32|
|INT_32|INT32|int32|
|INT_64|INT64|int64|
|UINT_8|INT32|uint32|
|UINT_16|INT32|uint32|
|UINT_32|INT32|uint32|
|UINT_64|INT64|uint64|
|DATE|INT32|int32|
|TIME_MILLIS|INT32|int32|
|TIME_MICROS|INT64|int64|
|TIMESTAMP_MILLIS|INT64|int64|
|TIMESTAMP_MICROS|INT64|int64|
|INTERVAL|FIXED_LEN_BYTE_ARRAY|string|
|DECIMAL|INT32,INT64,FIXED_LEN_BYTE_ARRAY,BYTE_ARRAY|int32,int64,string,string|
|LIST||slice||
|MAP||map||

### Tips
* Although DECIMAL can be stored as INT32,INT64,FIXED_LEN_BYTE_ARRAY,BYTE_ARRAY, Currently I suggest to use FIXED_LEN_BYTE_ARRAY. 

## Encodings

#### PLAIN:
All types  
#### PLAIN_DICTIONARY:
All types  
#### DELTA_BINARY_PACKED:
INT32, INT64, INT_8, INT_16, INT_32, INT_64, UINT_8, UINT_16, UINT_32, UINT_64, TIME_MILLIS, TIME_MICROS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS  
#### DELTA_BYTE_ARRAY:
BYTE_ARRAY, UTF8  
#### DELTA_LENGTH_BYTE_ARRAY:
BYTE_ARRAY, UTF8

### Tips
* Some platforms don't support all kinds of encodings. If you are not sure, just use PLAIN and PLAIN_DICTIONARY.


## Repetition Types
There are three repetition types in Parquet: REQUIRED, OPTIONAL, REPEATED. 

|Repetition Type|Example|Description|
|-|-|-|
|REQUIRED|```V1 int32 `parquet:"name=v1, type=INT32"` ```|No extra description|
|OPTIONAL|```V1 *int32 `parquet:"name=v1, type=INT32"` ```|Declare as pointer|
|REPEATED|```V1 []int32 `parquet:"name=v1, type=INT32, repetitontype=REPEATED"` ```|Add 'repetitiontype=repeated' in tags|

### Tips
* The difference between a List and a REPEATED variable is the 'repetitiontype' in tags. Although both of them are stored as slice in go, they are different in parquet. You can find the detail of List in parquet at [here](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md). I suggest just use a List.


## Examples of Types and Encodings
```golang
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

```

## Read/Write
Read/Write a parquet file need a ParquetFile interface implemented
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
Using this interface, parquet-go can read/write parquet file on different plantforms. Currently local and HDFS interfaces are implemented.(It's not possible for S3, because it doesn't support random access.)



Following is a simple example of read/write parquet file on local disk. It can be found in example directory:
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
	Name   string  `parquet:"name=name, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Id     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
	Day    int32   `parquet:"name=day, type=DATE"`
}
func main() {
	fw, _ := ParquetFile.NewLocalFileWriter("flat.parquet")
	//write
	pw, _ := ParquetWriter.NewParquetWriter(fw, new(Student), 10)
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
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()

	///read 
	fr, _ := ParquetFile.NewLocalFileReader("flat.parquet")
	pr, err := ParquetReader.NewParquetReader(fr, new(Student), 1)
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
This plugin is used for data format similar with CSV(not nested). The format of the schema is same with before.

#### Example
```golang
func main() {
	md := []string{
		"name=Name, type=UTF8, encoding=PLAIN_DICTIONARY",
		"name=Age, type=INT32",
		"name=Id, type=INT64",
		"name=Weight, type=FLOAT",
		"name=Sex, type=BOOLEAN",
	}

	//write
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
			ParquetType.BYTE_ARRAY("Student Name"),
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

### JSONWriter Plugin
JSONWriter can convert JSON strings to parquet by the parquet schema, which is also a JSON string. The schema format is
```json
{
    "Tag":"name=name, type=UTF8",
    "Fields":[]
}
```

#### Example

```golang
func main() {
    md := `
    {
        "Tag":"name=parquet-go-root",
        "Fields":[
            {"Tag":"name=name, type=UTF8, encoding=PLAIN_DICTIONARY"},
            {"Tag":"name=age, type=INT32"},
            {"Tag":"name=id, type=INT64"},
            {"Tag":"name=weight, type=FLOAT"},
            {"Tag":"name=sex, type=BOOLEAN"},
            {"Tag":"name=classes, type=LIST",
             "Fields":[
                  {"Tag":"name=element, type=UTF8"}
              ]
            },
            {"Tag":"name=scores, type=MAP",
             "Fields":[
                 {"Tag":"name=key, type=UTF8"},
                 {"Tag":"name=value, type=LIST",
                  "Fields":[{"Tag":"name=element, type=FLOAT"}]
                 }
             ]
            },
            {"Tag":"name=friends, type=UTF8, repetitiontype=REPEATED"}
        ]
    }
`
	//write
	fw, _ := ParquetFile.NewLocalFileWriter("json.parquet")
	pw, _ := JSONWriter.NewJSONWriter(md, fw, 1)

	num := 10
	for i := 0; i < num; i++ {
		rec := `
            {
                "name":"%s",
                "age":%d,
                "id":%d,
                "weight":%f,
                "sex":%t,
                "classes":["Math", "Computer", "English"],
                "scores":{
                            "Math":[99.5, 98.5, 97],
                            "Computer":[98,97.5],
                            "English":[100]
                         },
                "friends":["aa","bb"]
            }
        `
		rec = fmt.Sprintf(rec, "Student Name", 20+i%5, i, 50.0+float32(i)*0.1, i%2 == 0)
		pw.Write(rec)
	}
	pw.Flush(true)
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()
}

```

## Status
Here are a few todo items. Welcome any help!
* Performance Test(Issue14)
* Test in different platforms

Please start to use it and give feedback. Help is needed and anything is welcome.