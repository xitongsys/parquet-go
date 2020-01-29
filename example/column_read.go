package main

import (
	"log"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
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
	fw, err := local.NewLocalFileWriter("column.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := 
	writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer")
		return
	}
	num := int64(10)
	for i := 0; int64(i) < num; i++ {
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
	fr, err := local.NewLocalFileReader("column.parquet")
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pr, err := reader.NewParquetColumnReader(fr, 4)
	if err != nil {
		log.Println("Can't create column reader", err)
		return
	}
	num = int64(pr.GetNumRows())

	pr.SkipRowsByPath("parquet_go_root.name", 5) //skip the first five rows
	names, rls, dls, err = pr.ReadColumnByPath("parquet_go_root.name", num)
	log.Println("name", names, rls, dls, err)

	classes, rls, dls, err = pr.ReadColumnByPath("parquet_go_root.class.list.element", num)
	log.Println("class", classes, rls, dls, err)

	scores_key, rls, dls, err = pr.ReadColumnByPath("parquet_go_root.score.key_value.key", num)
	scores_value, rls, dls, err = pr.ReadColumnByPath("parquet_go_root.score.key_value.value", num)
	log.Println("parquet_go_root.scores_key", scores_key, err)
	log.Println("parquet_go_root.scores_value", scores_value, err)

	pr.SkipRowsByIndex(2, 5) //skip the first five rows
	ids, _, _, _ = pr.ReadColumnByIndex(2, num)
	log.Println(ids)

	pr.ReadStop()
	fr.Close()

}
