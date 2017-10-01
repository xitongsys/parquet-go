# parquet-go v0.4
parquet-go is a pure-go implementation of reading and writing the parquet format file. 
* Support Read/Write Nested/Flat Parquet File
* Support all Types in Parquet
* Very simple to use (like json marshal/unmarshal)

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
The variables which will read/write from/to a parquet file must be declared as these types.
OPTIONAL variables are declared as pointers.

## Core Data Structure
The core data structure named "Table":
```
type Table struct {
	Repetition_Type    parquet.FieldRepetitionType
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

## Marshal/Unmarshal
Marshal/Unmarshal functions are used to encode/decode the parquet file. 
Marshl convert a struct slice to a ```*map[string]*Table```
Unmarshal convert a ```*map[string]*Table``` to a struct slice

### Marshal Example
```
stus := make([]Student, 0)
stus = append(stus, stu01, stu02)
src := Marshal(stus, 0, len(stus), schemaHandler)
```

### Unmarshal Example
```
dst := make([]Student, 0)
Unmarshal(src, &dst, schemaHandler)
```

## Read/Write
read/write a parquet file need a ParquetFile interface implemented
```
type ParquetFile interface {
	Seek(offset int, pos int) (int64, error)
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close()
	Open(name string) error
	Create(name string) error
}
```
Using this interface, parquet-go can read/write parquet file on any plantform(local/hdfs/s3...)

The following is a simple example which can be found in example directory:
```
...

type MyFile struct {
	file *os.File
}

func (self *MyFile) Create(name string) error {
	file, err := os.Create(name)
	self.file = file
	return err
}
func (self *MyFile) Open(name string) error {
	file, err := os.Open(name)
	self.file = file
	return err
}
func (self *MyFile) Seek(offset int, pos int) (int64, error) {
	return self.file.Seek(int64(offset), pos)
}

func (self *MyFile) Read(b []byte) (n int, err error) {
	return self.file.Read(b)
}

func (self *MyFile) Write(b []byte) (n int, err error) {
	return self.file.Write(b)
}

func (self *MyFile) Close() {
	self.file.Close()
}

func main() {
	var f ParquetFile
	f = &MyFile{}

	//write flat
	f.Create("flat.parquet")
	ph := NewParquetHandler()
	ph.WriteInit(f, new(Student), 20)

	num := 10
	id := 1
	stuName := "aaaaaaaaaa"

	for i := 0; i < num; i++ {
		stu := Student{
			Name:   UTF8(stuName),
			Age:    INT32(i),
			Id:     INT64(id),
			Weight: FLOAT(50.0 + float32(i)*0.1),
			Sex:    BOOLEAN(i%2 == 0),
		}
		stuName = nextName(stuName)
		id++
		ph.Write(stu)

	}
	ph.WriteStop()
	log.Println("Write Finished")
	f.Close()


	///read flat
	f.Open("flat.parquet")
	ph = NewParquetHandler()
	rowGroupNum := ph.ReadInit(f)
	for i := 0; i < rowGroupNum; i++ {
		stus := make([]Student, 0)
		tmap := ph.ReadOneRowGroup()
		Unmarshal(tmap, &stus, ph.SchemaHandler)
		log.Println(stus)
	}

	f.Close()
}

```

## Parallel
Write functions have a parallel parameters np which is the number of goroutines in writing.
```
func (self *ParquetHandler) WriteInit(pfile ParquetFile, obj interface{}, np int64)
```


## Note
* Have tested the parquet file written by parquet-go on many big data plantform (Spark/Hive/Presto), everything is ok :)
* Almost all the features of the parquet are provided now.

## To do
* Optimize performance
