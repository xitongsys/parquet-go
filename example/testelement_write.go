package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/writer"
)

type subElem struct {
	Val string
}

type testNestedElem struct {
	SubElem     subElem
	SubPtr      *subElem
	SubList     []subElem
	SubRepeated []*subElem
}

func main() {
	testNestedElems := []interface{}{
		testNestedElem{},
		testNestedElem{SubElem: subElem{Val: "hi"}},
		testNestedElem{SubPtr: &subElem{Val: "hi"}},
		testNestedElem{SubList: []subElem{}},
		testNestedElem{SubList: []subElem{{Val: "hi"}}},
		testNestedElem{SubList: []subElem{{Val: "hi"}, {}, {Val: "there"}}},
		testNestedElem{SubRepeated: []*subElem{}},
		testNestedElem{SubRepeated: []*subElem{{Val: "hi"}}},
		testNestedElem{SubRepeated: []*subElem{{Val: "hi"}, nil, {Val: "there"}}},
	}
	fw, err := local.NewLocalFileWriter("output/testelement.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	schemaHandler, err := schema.NewSchemaHandlerFromProtoStruct(new(testNestedElem))
	if err != nil {
		log.Println("failed to create the schema handler: %v", err)
	}
	pw, err := writer.NewParquetWriter(fw, schemaHandler, 1)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	for _, stu := range testNestedElems {
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
			return
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	fw.Close()
	log.Println("Write Finished")
}
