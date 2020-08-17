package main

import (
	"bytes"
	source "github.com/xitongsys/parquet-go-source/http"
	"github.com/xitongsys/parquet-go/reader"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
)

type Student struct {
	Name   string  `parquet:"name=name, type=UTF8"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Id     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
	path := "flat.parquet.snappy"
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// create sample request
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("parquet_file", filepath.Base(path))
	if err != nil {
		log.Fatal(err)
	}
	_, err = io.Copy(part, file)
	err = writer.Close()
	if err != nil {
		log.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// parse request body
	reqFile, fileHeader, err := req.FormFile("parquet_file")
	if err != nil {
		log.Fatal(err)
	}
	defer reqFile.Close()

	// create readers
	fr := source.NewMultipartFileWrapper(fileHeader, reqFile)
	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Fatal(err)
	}
	defer pr.ReadStop()

	// read contents
	num := int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]Student, 1)
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}
}
