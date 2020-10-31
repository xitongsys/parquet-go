# parquet-go

[![Travis Status for xitongsys/parquet-go](https://travis-ci.org/xitongsys/parquet-go.svg?branch=master&label=linux+build)](https://travis-ci.org/xitongsys/parquet-go)
[![godoc for xitongsys/parquet-go](https://godoc.org/github.com/nathany/looper?status.svg)](http://godoc.org/github.com/xitongsys/parquet-go)

parquet-go is a pure-go implementation of reading and writing the parquet format file.

* Support Read/Write Nested/Flat Parquet File
* Simple to use
* High performance

## Install

Add the parquet-go library to your $GOPATH/src and install dependencies:

```sh
go get github.com/xitongsys/parquet-go
```

## Examples

The `example/` directory contains several examples.

The `local_flat.go` example creates some data and writes it out to the `example/output/flat.parquet` file.

```sh
cd $GOPATH/src/github.com/xitongsys/parquet-go/example
go run local_flat.go
```

The `local_flat.go` code shows how it's easy to output `structs` from Go programs to Parquet files.

## Type

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
|INT_8|INT32|int8|
|INT_16|INT32|int16|
|INT_32|INT32|int32|
|INT_64|INT64|int64|
|UINT_8|INT32|uint8|
|UINT_16|INT32|uint16|
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

* Parquet-go supports type alias such `type MyString string`. But the base type must follow the table instructions.

## Encoding

#### PLAIN:

All types

#### PLAIN_DICTIONARY/RLE_DICTIONARY:

All types

#### DELTA_BINARY_PACKED:

INT32, INT64, INT_8, INT_16, INT_32, INT_64, UINT_8, UINT_16, UINT_32, UINT_64, TIME_MILLIS, TIME_MICROS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS

#### DELTA_BYTE_ARRAY:

BYTE_ARRAY, UTF8

#### DELTA_LENGTH_BYTE_ARRAY:

BYTE_ARRAY, UTF8

### Tips

* Some platforms don't support all kinds of encodings. If you are not sure, just use PLAIN and PLAIN_DICTIONARY.
* If the fields have many different values, please don't use PLAIN_DICTIONARY encoding. Because it will record all the different values in a map which will use a lot of memory. Actually it use a 32-bit integer to store the index. It can not used if your unique values number is larger than 32-bit.
* Large array values may be duplicated as min and max values in page stats, significantly increasing file size. If stats are not useful for such a field, they can be omitted from written files by adding `omitstats=true` to a field tag.

## Repetition Type

There are three repetition types in Parquet: REQUIRED, OPTIONAL, REPEATED.

|Repetition Type|Example|Description|
|-|-|-|
|REQUIRED|```V1 int32 `parquet:"name=v1, type=INT32"` ```|No extra description|
|OPTIONAL|```V1 *int32 `parquet:"name=v1, type=INT32"` ```|Declare as pointer|
|REPEATED|```V1 []int32 `parquet:"name=v1, type=INT32, repetitontype=REPEATED"` ```|Add 'repetitiontype=REPEATED' in tags|

### Tips

* The difference between a List and a REPEATED variable is the 'repetitiontype' in tags. Although both of them are stored as slice in go, they are different in parquet. You can find the detail of List in parquet at [here](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md). I suggest just use a List.
* For LIST and MAP, some existed parquet files use some nonstandard formats(see [here](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)). For standard format, parquet-go will convert them to go slice and go map. For nonstandard formats, parquet-go will convert them to corresponding structs.

## Example of Type and Encoding

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
Int_8           int8  `parquet:"name=int_8, type=INT_8"`
Int_16          int16  `parquet:"name=int_16, type=INT_16"`
Int_32          int32  `parquet:"name=int_32, type=INT_32"`
Int_64          int64  `parquet:"name=int_64, type=INT_64"`
Uint_8          uint8 `parquet:"name=uint_8, type=UINT_8"`
Uint_16         uint16 `parquet:"name=uint_16, type=UINT_16"`
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

## Compression Type

|Type|Support|
|-|-|
| CompressionCodec_UNCOMPRESSED | YES|
|CompressionCodec_SNAPPY|YES|
|CompressionCodec_GZIP|YES|
|CompressionCodec_LZO|NO|
|CompressionCodec_BROTLI|NO|
|CompressionCodec_LZ4 |NO|
|CompressionCodec_ZSTD|YES|

## ParquetFile

Read/Write a parquet file need a ParquetFile interface implemented

```golang
type ParquetFile interface {
	io.Seeker
	io.Reader
	io.Writer
	io.Closer
	Open(name string) (ParquetFile, error)
	Create(name string) (ParquetFile, error)
}
```

Using this interface, parquet-go can read/write parquet file on different platforms. All the file sources are at [parquet-go-source](https://github.com/xitongsys/parquet-go-source). Now it supports(local/hdfs/s3/gcs/memory).

## Writer

Three Writers are supported: ParquetWriter, JSONWriter, CSVWriter.

* ParquetWriter is used to write predefined Golang structs.
[Example of ParquetWriter](https://github.com/xitongsys/parquet-go/blob/master/example/local_flat.go)

* JSONWriter is used to write JSON strings
[Example of JSONWriter](https://github.com/xitongsys/parquet-go/blob/master/example/json_write.go)

* CSVWriter is used to write data format similar with CSV(not nested)
[Example of CSVWriter](https://github.com/xitongsys/parquet-go/blob/master/example/csv_write.go)

## Reader

Two Readers are supported: ParquetReader, ColumnReader

* ParquetReader is used to read predefined Golang structs
[Example of ParquetReader](https://github.com/xitongsys/parquet-go/blob/master/example/local_nested.go)

* ColumnReader is used to read raw column data. The read function return 3 slices([value], [RepetitionLevel], [DefinitionLevel]) of the records.
[Example of ColumnReader](https://github.com/xitongsys/parquet-go/blob/master/example/column_read.go)

### Tips

* If the parquet file is very big (even the size of parquet file is small, the uncompressed size may be very large), please don't read all rows at one time, which may induce the OOM. You can read a small portion of the data at a time like a stream-oriented file.

* `RowGroupSize` and `PageSize` may influence the final parquet file size. You can find the details from [here](https://github.com/apache/parquet-format). You can reset them in ParquetWriter
```go
	pw.RowGroupSize = 128 * 1024 * 1024 // default 128M
	pw.PageSize = 8 * 1024 // default 8K
```

## Schema

There are three methods to define the schema: go struct tags, Json, CSV metadata. Only items in schema will be written and others will be ignored.

### Tag

```golang
type Student struct {
	Name   string  `parquet:"name=name, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Id     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
	Day    int32   `parquet:"name=day, type=DATE"`
}
```

[Example of tags](https://github.com/xitongsys/parquet-go/blob/master/example/local_flat.go)

### JSON

JSON schema can be used to define some complicated schema, which can't be defined by tag.

```golang
type Student struct {
	Name    string
	Age     int32
	Id      int64
	Weight  float32
	Sex     bool
	Classes []string
	Scores  map[string][]float32

	Friends []struct {
		Name string
		Id   int64
	}
	Teachers []struct {
		Name string
		Id   int64
	}
}

var jsonSchema string = `
{
  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
  "Fields": [
    {"Tag": "name=name, inname=Name, type=UTF8, repetitiontype=REQUIRED"},
    {"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"},
    {"Tag": "name=id, inname=Id, type=INT64, repetitiontype=REQUIRED"},
    {"Tag": "name=weight, inname=Weight, type=FLOAT, repetitiontype=REQUIRED"},
    {"Tag": "name=sex, inname=Sex, type=BOOLEAN, repetitiontype=REQUIRED"},

    {"Tag": "name=classes, inname=Classes, type=LIST, repetitiontype=REQUIRED",
     "Fields": [{"Tag": "name=element, type=UTF8, repetitiontype=REQUIRED"}]
    },
    {
      "Tag": "name=scores, inname=Scores, type=MAP, repetitiontype=REQUIRED",
      "Fields": [
        {"Tag": "name=key, type=UTF8, repetitiontype=REQUIRED"},
        {"Tag": "name=value, type=LIST, repetitiontype=REQUIRED",
         "Fields": [{"Tag": "name=element, type=FLOAT, repetitiontype=REQUIRED"}]
        }
      ]
    },
    {
      "Tag": "name=friends, inname=Friends, type=LIST, repetitiontype=REQUIRED",
      "Fields": [
       {"Tag": "name=element, repetitiontype=REQUIRED",
        "Fields": [
         {"Tag": "name=name, inname=Name, type=UTF8, repetitiontype=REQUIRED"},
         {"Tag": "name=id, inname=Id, type=INT64, repetitiontype=REQUIRED"}
        ]}
      ]
    },
    {
      "Tag": "name=teachers, inname=Teachers, repetitiontype=REPEATED",
      "Fields": [
        {"Tag": "name=name, inname=Name, type=UTF8, repetitiontype=REQUIRED"},
        {"Tag": "name=id, inname=Id, type=INT64, repetitiontype=REQUIRED"}
      ]
    }
  ]
}
`
```
[Example of JSON schema](https://github.com/xitongsys/parquet-go/blob/master/example/json_schema.go)


### CSV metadata

```golang
md := []string{
	"name=Name, type=UTF8, encoding=PLAIN_DICTIONARY",
	"name=Age, type=INT32",
	"name=Id, type=INT64",
	"name=Weight, type=FLOAT",
	"name=Sex, type=BOOLEAN",
}
```

[Example of CSV metadata](https://github.com/xitongsys/parquet-go/blob/master/example/csv_write.go)

### Tips

* Parquet-go reads data as an object in Golang and every field must be a public field, which start with an upper letter. This field name we call it `InName`. Field name in parquet file we call it `ExName`. Function `common.HeadToUpper` converts `ExName` to `InName`. There are some restriction:
1. It's not allowed if two field names are only different at their first letter case. Such as `name` and `Name`.
2. `PARGO_PREFIX_` is a reserved string, which you'd better not use it as a name prefix. ([#294](https://github.com/xitongsys/parquet-go/issues/294))

## Parallel

Read/Write initial functions have a parallel parameters np which is the number of goroutines in reading/writing.

```golang
func NewParquetReader(pFile ParquetFile.ParquetFile, obj interface{}, np int64) (*ParquetReader, error)
func NewParquetWriter(pFile ParquetFile.ParquetFile, obj interface{}, np int64) (*ParquetWriter, error)
func NewJSONWriter(jsonSchema string, pfile ParquetFile.ParquetFile, np int64) (*JSONWriter, error)
func NewCSVWriter(md []string, pfile ParquetFile.ParquetFile, np int64) (*CSVWriter, error)
```

## Examples

|Example file|Descriptions|
|-|-|
|[local_flat.go](https://github.com/xitongsys/parquet-go/blob/master/example/local_flat.go)|write/read parquet file with no nested struct|
|[local_nested.go](https://github.com/xitongsys/parquet-go/blob/master/example/local_nested.go)|write/read parquet file with nested struct|
|[read_partial.go](https://github.com/xitongsys/parquet-go/blob/master/example/read_partial.go)|read partial fields from a parquet file|
|[read_partial2.go](https://github.com/xitongsys/parquet-go/blob/master/example/read_partial2.go)|read sub-struct from a parquet file|
|[read_without_schema_predefined.go](https://github.com/xitongsys/parquet-go/blob/master/example/read_without_schema_predefined.go)|read a parquet file and no struct/schema predefined needed|
|[read_partial_without_schema_predefined.go](https://github.com/xitongsys/parquet-go/blob/master/example/read_partial_without_schema_predefined.go)|read sub-struct from a parquet file and no struct/schema predefined needed|
|[json_schema.go](https://github.com/xitongsys/parquet-go/blob/master/example/json_schema.go)|define schema using json string|
|[json_write.go](https://github.com/xitongsys/parquet-go/blob/master/example/json_write.go)|convert json to parquet|
|[convert_to_json.go](https://github.com/xitongsys/parquet-go/blob/master/example/convert_to_json.go)|convert parquet to json|
|[csv_write.go](https://github.com/xitongsys/parquet-go/blob/master/example/csv_write.go)|special csv writer|
|[column_read.go](https://github.com/xitongsys/parquet-go/blob/master/example/column_read.go)|read raw column data and return value,repetitionLevel,definitionLevel|
|[type.go](https://github.com/xitongsys/parquet-go/blob/master/example/type.go)|example for schema of types|
|[type_alias.go](https://github.com/xitongsys/parquet-go/blob/master/example/type_alias.go)|example for type alias|
|[writer.go](https://github.com/xitongsys/parquet-go/blob/master/example/writer.go)|create ParquetWriter from io.Writer|


## Tool

* [parquet-tools](https://github.com/xitongsys/parquet-go/blob/master/tool/parquet-tools): Command line tools that aid in the inspection of Parquet files

Please start to use it and give feedback or just star it! Help is needed and anything is welcome.

