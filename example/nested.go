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

func main() {
	flag.Parse()

	w, closer, err := NewParquetWriter(*outFile)
	if err != nil {
		log.Fatalf("unable to make parquet writer for %s\n\t%s", *outFile, err.Error)
	}
	defer closer()

	r1 := &Data{
		Symbol: "A",
		M: map[string][]*ExpData{
			"a1": []*ExpData{
				&ExpData{
					Ask: 13,
					D: []*D{
						&D{
							Symbol:           "A",
							DeliverableUnits: 20.0,
						},
					},
				},
			},
		},
	}
	r2 := &Data{
		Symbol: "B",
		M: map[string][]*ExpData{
			"b1": []*ExpData{
				&ExpData{
					Ask: 12.2,
					D:   make([]*D, 0),
				},
			},
		},
	}
	if err = w.Write(r1); err != nil {
		log.Fatal(err)
	}
	if err = w.Write(r2); err != nil {
		log.Fatal(err)
	}
}

// NewParquetWriter creates default writer and returns it and closer
func NewParquetWriter(fname string) (*ParquetWriter.ParquetWriter, func() error, error) {
	fw, err := ParquetFile.NewLocalFileWriter(fname)
	if err != nil {
		return nil, nil, fmt.Errorf("Can't create local file (%s)\n%s", fname, err.Error())
	}

	pw, err := ParquetWriter.NewParquetWriter(fw, new(Data), 1)
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

// Data ...
type Data struct {
	Symbol string                `parquet:"name=symbol, type=UTF8, encoding=PLAIN_DICTIONARY" json:"symbol"`
	M      map[string][]*ExpData `parquet:"name=M, type=MAP, keytype=UTF8"`
}

// ExpData ...
type ExpData struct {
	Ask float64 `parquet:"name=ask, type=DOUBLE" json:"ask"`
	D   []*D    `parquet:"name=d, type=LIST" json:"d"`
}

// D ...
type D struct {
	Symbol           string  `parquet:"name=symbol, type=UTF8, encoding=PLAIN_DICTIONARY" json:"symbol"`
	DeliverableUnits float64 `parquet:"name=deliverableUnits, type=DOUBLE" json:"deliverableUnits"`
	CurrencyType     string  `parquet:"name=currencyType, type=UTF8, encoding=PLAIN_DICTIONARY" json:"currencyType"`
}
