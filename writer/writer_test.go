package writer

import (
	"testing"

	"github.com/xitongsys/parquet-go-source/buffer"
)

func TestWriteByteSlice(t *testing.T) {
	type Uint8Repeated struct {
		Bytes []byte `parquet:"name=bytes, type=INT32, convertedtype=UINT_8, repetitiontype=REPEATED"`
	}
	bytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	fw, err := buffer.NewBufferFile(nil)
	if err != nil {
		t.Fatalf("failed to create buffer file: %v", err)
	}
	defer fw.Close()

	pw, err := NewParquetWriter(fw, &Uint8Repeated{}, 1)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	for i := 0; i < 1000; i++ {
		if err := pw.Write(&Uint8Repeated{bytes}); err != nil {
			t.Errorf("failed to write element: %v", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		t.Fatalf("failed to stop writing: %v", err)
	}
}
