package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ncw/swift"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/xitongsys/parquet-go-source/swift"
)

type Student struct {
	Name   string  `parquet:"name=name, type=UTF8"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Id     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
	Day    int32   `parquet:"name=day, type=DATE"`
}

func main() {
	connection := swift.Connection{
		UserName: "test_user",
		ApiKey:   "passw0rd",
		AuthUrl:  "http://localhost:35357/v3",
		Domain:   "Default",
		Tenant:   "test_project",
	}
	if err := connection.Authenticate(); err != nil {
		log.Print("Failed to authenticate to keystone: ", err)
		return
	}

	containerName := "swift-parquet-test"
	fileName := "flat.parquet"
	num := 100

	log.Println("Write started")
	fw, err := swiftsource.NewSwiftFileWriter(containerName, fileName, &connection)
	if err != nil {
		log.Println("Failed to create file: ", err)
		return
	}

	//write
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Failed to create parquet writer: ", err)
		return
	}

	for i := 0; i < num; i++ {
		stu := Student{
			Name:   fmt.Sprintf("Student-%d", i),
			Age:    int32(20 + i%5),
			Id:     int64(i),
			Weight: 50.0 + float32(i)*0.1,
			Sex:    i%2 == 0,
			Day:    int32(time.Now().Unix() / 3600 / 24),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error: ", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error: ", err)
		return
	}

	if err = fw.Close(); err != nil {
		log.Println("Failed to close writer: ", err)
	}
	log.Println("Write finished")

	///read
	log.Println("Read started")
	fr, err := swiftsource.NewSwiftFileReader(containerName, fileName, &connection)
	if err != nil {
		log.Println("Failed to open file: ", err)
		return
	}

	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Failed to create parquet reader: ", err)
		return
	}
	num = int(pr.GetNumRows())
	for i := 0; i < num/10; i++ {
		stus := make([]Student, 10) //read 10 rows
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error: ", err)
		}
		log.Println(stus)
	}
	pr.ReadStop()
	if err = fr.Close(); err != nil {
		log.Println("Failed to close reader: ", err)
	}
	log.Println("Read finished")
}
