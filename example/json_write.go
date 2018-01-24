package main

import (
	"fmt"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/Plugin/JSONWriter"
	"log"
)

func main() {
	md := `
    {
        "Tag":"name=parquet-go-root",
        "Fields":[
		    {"Tag":"name=name, type=UTF8, repetitiontype=OPTIONAL"},
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
            {"Tag":"name=friends, type=LIST",
             "Fields":[
                 {"Tag":"name=element",
                  "Fields":[
                      {"Tag":"name=name, type=UTF8"},
                      {"Tag":"name=id, type=INT64"}
                  ]
                 }
             ]
            },
            {"Tag":"name=teachers, repetitiontype=REPEATED",
             "Fields":[
                 {"Tag":"name=name, type=UTF8"},
                 {"Tag":"name=id, type=INT64"}
             ]
            }
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
                "friends":[
                    {"name":"friend1", "id":1},
                    {"name":"friend2", "id":2}
                ],
                "teachers":[
                    {"name":"teacher1", "id":1},
                    {"name":"teacher2", "id":2}
                ]
            }
        `

		rec = fmt.Sprintf(rec, "Student Name", 20+i%5, i, 50.0+float32(i)*0.1, i%2 == 0)

		pw.Write(rec)

	}
	pw.WriteStop()
	log.Println("Write Finished")
	fw.Close()

}
