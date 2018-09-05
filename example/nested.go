package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
)

var outFile = flag.String("output", "res.parquet", "the file to write the generated files to.")

// TestData ...
type TestData struct {
	Status string             `parquet:"name=status, type=UTF8, encoding=PLAIN_DICTIONARY" json:"status"`
	M      map[string][]*Data `parquet:"name=M, type=MAP, keytype=UTF8"`
}

// Data ...
type Data struct {
	Ask float64         `parquet:"name=ask, type=DOUBLE" json:"ask"`
	D   []*Deliverables `parquet:"name=emptyList, type=LIST" json:"emptyList"`
}

// Deliverables ...
type Deliverables struct {
	Unused string `parquet:"name=unused, type=UTF8, encoding=PLAIN_DICTIONARY" json:"unused"`
}

func main() {
	flag.Parse()

	w, closer, err := NewParquetWriter(*outFile)
	defer closer()
	if err != nil {
		log.Fatalf("unable to make parquet writer for %s\n\t%s", *outFile, err.Error)
	}

	r1 := &TestData{
		Status: "Success",
		M: map[string][]*Data{
			"A": []*Data{
				&Data{
					Ask: 1,
					D:   make([]*Deliverables, 0),
				},
			},
		},
	}
	r2 := &TestData{
		Status: "Success",
		M: map[string][]*Data{
			"A": []*Data{
				&Data{
					Ask: 2,
					D:   make([]*Deliverables, 0),
				},
			},
		},
	}

	if err = w.Write(r1); err != nil {
		log.Fatal("R1: ", err)
	}
	if err = w.Write(r2); err != nil {
		log.Fatal("R2: ", err)
	}
}

// NewParquetWriter creates default writer and returns it and closer
func NewParquetWriter(fname string) (*ParquetWriter.ParquetWriter, func() error, error) {
	fw, err := ParquetFile.NewLocalFileWriter(fname)
	if err != nil {
		return nil, nil, fmt.Errorf("Can't create local file (%s)\n%s", fname, err.Error())
	}

	pw, err := ParquetWriter.NewParquetWriter(fw, new(TestData), 1)
	if err != nil {
		return nil, nil, fmt.Errorf("Can't create parquet writer for %s\n%s", fname, err.Error())
	}
	pw.RowGroupSize = 256 * 1024 * 1024 //256M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	return pw, func() error {
		log.Print("closing parquet writer")
		if err = pw.WriteStop(); err != nil {
			return fmt.Errorf("WriteStop error for %s\n%s", fname, err.Error())
		}
		log.Print("Parquet writer closed for: ", fname)
		return fw.Close()
	}, nil
}
