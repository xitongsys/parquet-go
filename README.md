# parquet-go v0.3
parquet-go is a pure-go implementation of reading and writing the parquet format file. 
* Support Read/Write Nested/Flat Parquet File
* Support all Types in Parquet
* Very simple to use

## Required
* git.apache.org/thrift.git/lib/go/thrift
* github.com/golang/snappy

## Types
There are two Types in Parquet: Base Type and Logical Type
They are defined in ParquetType.go as following:
```
//base type
type BOOLEAN bool
type INT32 int32
type INT64 int64
type INT96 string // length=96
type FLOAT float32
type DOUBLE float64
type BYTE_ARRAY string
type FIXED_LEN_BYTE_ARRAY string

//logical type
type UTF8 string
type INT_8 int32
type INT_16 int32
type INT_32 int32
type INT_64 int64
type UINT_8 uint32
type UINT_16 uint32
type UINT_32 uint32
type UINT_64 uint64
type DATE int32
type TIME_MILLIS int32
type TIME_MICROS int64
type TIMESTAMP_MILLIS int64
type TIMESTAMP_MICROS int64
type INTERVAL string // length=12
type DECIMAL string

```

## Example

### Read Parquet File
```
func ReadParquet(fname string) {
	file, _ := os.Open(fname)
	defer file.Close()

	res := parquet_go.Reader(file)
	for _, v := range res {
		fmt.Println(v.Path)
		for i, v2 := range v.Values {
			if reflect.TypeOf(v2) == reflect.TypeOf([]uint8{}) {
				fmt.Print(string(v2.([]byte)))
			} else {
				fmt.Print(v2)
			}
			fmt.Printf(" %d %d\n", v.DefinitionLevels[i], v.RepetitionLevels[i])
		}
	}
}
```

### Write Parquet File
```
type Student struct{
......
}

stus := make([]Student,10000)
......

file, _ := os.Create("nested.parquet")
parquet_go.WriteTo(file, stus, schemaHandler)	

```

## Note
* Have tested the parquet file written by parquet-go on many big data platform (Spark/Hive/Presto), everything is ok :)
* Not all the features of the parquet are provided now, so read some parquet file written by other programs may cause some failures.

## To do
* Add more features
* Parallel
* Optimize performance