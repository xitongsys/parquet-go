## Introduction
parquet-go is a pure-go implementation of reading and writing the parquet format file. 
* Support Read/Write Nested Parquet File
* Simple to Use

## Required
* git.apache.org/thrift.git/lib/go/thrift
* github.com/golang/snappy

## Example

# Read Parquet File
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

# Write Parquet File
```
type Student struct{
......
}

stus := make([]Student,10000)
......

file, _ := os.Create("nested.parquet")
parquet_go.WriteTo(file, stus, schemaHandler)	

```

