# parquet-go v0.8.5
parquet-go is a pure-go implementation of reading and writing the parquet format file. 
* Support Read/Write Nested/Flat Parquet File
* Support all Types in Parquet
* Very simple to use (like json marshal/unmarshal)
* High performance

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
	Open(name string) (ParquetFile, error)
	Create(name string) (ParquetFile, error)
}
```
Using this interface, parquet-go can read/write parquet file on any plantform(local/hdfs/s3...)
### Note:
* Open(name string) (ParquetFile, error) is used for read parquet. If name is "", it should return a new file handler of the same file.


The read and unmarshal processes can be separated and an example is shown in example/benchmark/ReadParquet.go  
In reading process, Unmarshal is a very time-consuming function. If this process is not needed, you can just get the table map and values by yourself.   

The following is a simple example of read/write parquet file on local disk. It can be found in example directory:
```
package main
import (
	. "github.com/xitongsys/parquet-go/ParquetHandler"
	. "github.com/xitongsys/parquet-go/ParquetType"
	"log"
	"os"
)
type Student struct {
	Name   UTF8
	Age    INT32
	Id     INT64
	Weight FLOAT
	Sex    BOOLEAN
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
	f, _ = f.Create("flat.parquet")
	ph := NewParquetHandler()
	ph.WriteInit(f, new(Student), 4, 30)

	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   UTF8("StudentName"),
			Age:    INT32(20 + i%5),
			Id:     INT64(i),
			Weight: FLOAT(50.0 + float32(i)*0.1),
			Sex:    BOOLEAN(i%2 == 0),
		}
		ph.Write(stu)
	}
	ph.Flush()
	ph.WriteStop()
	log.Println("Write Finished")
	f.Close()

	///read flat
	f, _ = f.Open("flat.parquet")
	ph = NewParquetHandler()
	rowGroupNum := ph.ReadInit(f, 10)
	for i := 0; i < rowGroupNum; i++ {
		stus := make([]Student, 0)
		ph.ReadOneRowGroupAndUnmarshal(&stus)
		log.Println(stus)
	}
	f.Close()
}

```

## Parallel
Read/Write initial functions have a parallel parameters np which is the number of goroutines in reading/writing.
```
func (self *ParquetHandler) ReadInit(pfile ParquetFile, np int64)
func (self *ParquetHandler) WriteInit(pfile ParquetFile, obj interface{}, np int64)
```

## Plugin
Plugin is used for some special purpose and will be added gradually.
### CSVWriter Plugin
This plugin is used for data format similar with CSV(not nested).
```
func main() {
	md := []MetadataType{
		{Type: "UTF8", Name: "Name"},
		{Type: "INT32", Name: "Age"},
		{Type: "INT64", Name: "Id"},
		{Type: "FLOAT", Name: "Weight"},
		{Type: "BOOLEAN", Name: "Sex"},
	}

	var f ParquetFile
	f = &MyFile{}

	//write flat
	f, _ = f.Create("csv.parquet")
	ph := NewCSVWriterHandler()
	ph.WriteInit(md, f, 10, 30)

	num := 10
	for i := 0; i < num; i++ {
		data := []string{
			"StudentName",
			fmt.Sprintf("%d", 20+i%5),
			fmt.Sprintf("%d", i),
			fmt.Sprintf("%f", 50.0+float32(i)*0.1),
			fmt.Sprintf("%t", i%2 == 0),
		}
		rec := make([]*string, len(data))
		for j := 0; j < len(data); j++ {
			rec[j] = &data[j]
		}
		ph.Flush()
		ph.Write(rec)
	}
	ph.WriteStop()
	log.Println("Write Finished")
	f.Close()
}
```

## Tips
### Uppercase/Lowercase of field name
In parquet-go the first letter of filed name must be uppercase. So the Marshal/Unmarshal functions can get the filed of the object. But there is no such restriction in other systems (e.g. Spark: support uppercase/lowercase; Hive: all the field names will convert to lowercase when load a parquet file, because Hive is not case sensitive).
  
Generally this isn't a problem in writing parquet, but I still provide a function 'NameToLower()' to convert the field names to lowercase when write parquet file. 
```
//write flat
f, _ = f.Create("flat.parquet")
ph := NewParquetHandler()
ph.WriteInit(f, new(Student), 4, 30)

num := 10
for i := 0; i < num; i++ {
	stu := Student{
		Name:   UTF8("StudentName"),
		Age:    INT32(20 + i%5),
		Id:     INT64(i),
		Weight: FLOAT(50.0 + float32(i)*0.1),
		Sex:    BOOLEAN(i%2 == 0),
		Day:    DATE(time.Now().Unix() / 3600 / 24),
	}
	ph.Write(stu)
}
ph.Flush()
ph.NameToLower()// convert the field name to lowercase
ph.WriteStop()
log.Println("Write Finished")
f.Close()

```

It is a problem in reading parquet file and it's solved in the following way:  
If the first letter of some field names are lowercase, you just need define a variable with a capitilized first letter. e.g.  
The field names in a parquet file is: nameofstudent, ageOfStudent, School_of_Student  
You need to define a struct as following:
```
type Student struct{
	 Nameofstudent UTF8 // nameofstudent
	 AgeOfStudent INT32 // ageOfStudent
	 School_of_Student UTF8 // School_of_Student
}
```

## Performance
A very simple performance test of writing/reading parquet did on Linux host (JRE 1.8.0, Golang 1.7.5, 23GB, 24 Cores). It is faster than java :)

Write Test Results  
![](https://github.com/xitongsys/parquet-go/blob/master/example/benchmark/WriteRes.png)

Read Test Results  
![](https://github.com/xitongsys/parquet-go/blob/master/example/benchmark/ReadRes.png)


