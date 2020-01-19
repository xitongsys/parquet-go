package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/xitongsys/parquet-go/parquet"
)

type Student struct {
	NameIn    string
	Age     int32
	Id      int64
	Weight  float32
	Sex     bool
	Classes []string
	Scores  map[string][]float32
	Ignored string

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
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
  "Fields": [
    {"Tag": "name=name, inname=NameIn, type=UTF8, repetitiontype=REQUIRED"},
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

func main() {
	var err error
	fw, err := local.NewLocalFileWriter("json_schema.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := writer.NewParquetWriter(fw, jsonSchema, 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			NameIn:    "StudentName",
			Age:     int32(20 + i%5),
			Id:      int64(i),
			Weight:  float32(50.0 + float32(i)*0.1),
			Sex:     bool(i%2 == 0),
			Classes: []string{"Math", "Physics"},
			Scores: map[string][]float32{
				"Math":    []float32{89.5, 99.4},
				"Physics": []float32{100.0, 95.3},
			},

			Friends: []struct {
				Name string
				Id   int64
			}{
				struct {
					Name string
					Id   int64
				}{
					Name: "Jack",
					Id:   01,
				},
			},

			Teachers: []struct {
				Name string
				Id   int64
			}{
				struct {
					Name string
					Id   int64
				}{
					Name: "Tom",
					Id:   02,
				},
			},
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()

	///read
	fr, err := local.NewLocalFileReader("json_schema.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, jsonSchema, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	
	num = int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}

	pr.ReadStop()
	fr.Close()

}
