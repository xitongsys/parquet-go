package reader

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/xitongsys/parquet-go-source/local"
)

func TestReader_Read(t *testing.T) {
	expectedRows := [] struct {
		Boolean bool
		Int     int32
		Long    int64
		Float   float32
		Double  float64
		Bytes   string
		String  string
	}{
		{
			Boolean: false,
			Int:     1,
			Long:    1,
			Float:   1.1,
			Double:  1.1,
			Bytes:   "foo",
			String:  "foo",
		},
		{
			Boolean: true,
			Int:     2,
			Long:    2,
			Float:   2.2,
			Double:  2.2,
			Bytes:   "bar",
			String:  "bar",
		},
	}
	f, err := local.NewLocalFileReader("testdata/primitives.parquet")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewParquetReader(f, nil, 1)
	if err != nil {
		t.Error(err)
	}

	if r.GetNumRows() != int64(len(expectedRows)) {
		t.Errorf("expected num rows %v, but actual %v", len(expectedRows), r.GetNumRows())
	}

	rows, err := r.ReadByNumber(int(r.GetNumRows()))
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(fmt.Sprint(rows), fmt.Sprint(expectedRows)) {
		t.Errorf("expected %v, but actual %v", expectedRows, rows)
	}
}
