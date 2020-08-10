package writer

import (
	"testing"

	"github.com/xitongsys/parquet-go-source/buffer"
)

func TestWriteCSV(t *testing.T) {
	md := []string{
		"name=First, type=UTF8, encoding=PLAIN",
		"name=Middle, type=UTF8, encoding=PLAIN",
		"name=Last, type=UTF8, encoding=PLAIN",
		"name=BirthCity, type=UTF8, encoding=PLAIN",
	}

	fw, err := buffer.NewBufferFile(nil)
	if err != nil {
		t.Fatal(err)
	}
	pw, err := NewCSVWriter(md, fw, 2)
	if err != nil {
		t.Fatal(err)
	}

	for j := 0; j < 10000; j++ {
		if err = pw.Write([]interface{}{"Harry", "S", "Truman", "Lamar"}); err != nil {
			t.Error(err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		t.Error(err)
	}
	if err := fw.Close(); err != nil {
		t.Error(err)
	}
}

func TestWriteCSVPlainDictionary(t *testing.T) {
	md := []string{
		"name=First, type=UTF8, encoding=PLAIN_DICTIONARY",
		"name=Middle, type=UTF8, encoding=PLAIN_DICTIONARY",
		"name=Last, type=UTF8, encoding=PLAIN_DICTIONARY",
		"name=BirthCity, type=UTF8, encoding=PLAIN_DICTIONARY",
	}
	fw, err := buffer.NewBufferFile(nil)
	if err != nil {
		t.Fatal(err)
	}
	pw, err := NewCSVWriter(md, fw, 2)
	if err != nil {
		t.Fatal(err)
	}

	for j := 0; j < 10000; j++ {
		if err = pw.Write([]interface{}{"Harry", "S", "Truman", "Lamar"}); err != nil {
			t.Error(err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		t.Error(err)
	}
	if err := fw.Close(); err != nil {
		t.Error(err)
	}
}

func BenchmarkWriteCSV(b *testing.B) {
	b.ReportAllocs()
	md := []string{
		"name=First, type=UTF8, encoding=PLAIN",
		"name=Middle, type=UTF8, encoding=PLAIN",
		"name=Last, type=UTF8, encoding=PLAIN",
		"name=BirthCity, type=UTF8, encoding=PLAIN",
	}
	for i := 0; i < b.N; i++ {
		fw, err := buffer.NewBufferFile(nil)
		if err != nil {
			b.Fatal(err)
		}
		pw, err := NewCSVWriter(md, fw, 2)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 10000; j++ {
			err = pw.Write([]interface{}{"Harry", "S", "Truman", "Lamar"})
			if err != nil {
				b.Fatal(err)
			}
		}
		err = pw.WriteStop()
		if err != nil {
			b.Fatal(err)
		}
		fw.Close()
	}
}

func BenchmarkWriteCSVPlainDictionary(b *testing.B) {
	b.ReportAllocs()
	md := []string{
		"name=First, type=UTF8, encoding=PLAIN_DICTIONARY",
		"name=Middle, type=UTF8, encoding=PLAIN_DICTIONARY",
		"name=Last, type=UTF8, encoding=PLAIN_DICTIONARY",
		"name=BirthCity, type=UTF8, encoding=PLAIN_DICTIONARY",
	}
	for i := 0; i < b.N; i++ {
		fw, err := buffer.NewBufferFile(nil)
		if err != nil {
			b.Fatal(err)
		}
		pw, err := NewCSVWriter(md, fw, 2)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 10000; j++ {
			err = pw.Write([]interface{}{"Harry", "S", "Truman", "Lamar"})
			if err != nil {
				b.Fatal(err)
			}
		}
		err = pw.WriteStop()
		if err != nil {
			b.Fatal(err)
		}
		fw.Close()
	}
}
