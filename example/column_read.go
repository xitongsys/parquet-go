package main

import (
	"log"
	"time"

	"github.com/nauto/parquet-go/ParquetFile"
	"github.com/nauto/parquet-go/ParquetReader"
	"github.com/nauto/parquet-go/ParquetWriter"
)

type Student struct {
	Name   string           `parquet:"name=name, type=UTF8"`
	Age    int32            `parquet:"name=age, type=INT32"`
	Id     int64            `parquet:"name=id, type=INT64"`
	Weight float32          `parquet:"name=weight, type=FLOAT"`
	Sex    bool             `parquet:"name=sex, type=BOOLEAN"`
	Day    int32            `parquet:"name=day, type=DATE"`
	Class  []string         `parquet:"name=class, type=SLICE, valuetype=UTF8"`
	Score  map[string]int32 `parquet:"name=score, type=MAP, keytype=UTF8, valuetype=INT32"`
}

func main() {
	var err error
	//write
	fw, err := ParquetFile.NewLocalFileWriter("column.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := ParquetWriter.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer")
		return
	}
	num := 10
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			Id:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Sex:    bool(i%2 == 0),
			Day:    int32(time.Now().Unix() / 3600 / 24),
			Class:  []string{"Math", "Physics", "Algorithm"},
			Score:  map[string]int32{"Math": int32(100 - i), "Physics": int32(100 - i), "Algorithm": int32(100 - i)},
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	log.Println("Write Finished")
	fw.Close()

	var names, classes, scores_key, scores_value, ids []interface{}
	var rls, dls []int32

	///read
	fr, err := ParquetFile.NewLocalFileReader("column.parquet")
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pr, err := ParquetReader.NewParquetColumnReader(fr, 4)
	if err != nil {
		log.Println("Can't create column reader", err)
		return
	}
	num = int(pr.GetNumRows())

	pr.SkipRowsByPath("name", 5) //skip the first five rows
	names, rls, dls = pr.ReadColumnByPath("name", num)
	log.Println("name", names, rls, dls)

	classes, rls, dls = pr.ReadColumnByPath("class.list.element", num)
	log.Println("class", classes, rls, dls)

	scores_key, rls, dls = pr.ReadColumnByPath("score.key_value.key", num)
	scores_value, rls, dls = pr.ReadColumnByPath("score.key_value.value", num)
	log.Println("scores_key", scores_key)
	log.Println("scores_value", scores_value)

	pr.SkipRowsByIndex(2, 5) //skip the first five rows
	ids, _, _ = pr.ReadColumnByIndex(2, num)
	log.Println(ids)

	pr.ReadStop()
	fr.Close()

}
