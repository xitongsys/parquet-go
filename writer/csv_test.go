package writer

import (
	"bytes"
	"testing"
)

type testBuffer struct {
	buf bytes.Buffer
}

func (b *testBuffer) Write(p []byte) (int, error) {
	return b.buf.Write(p)
}

func (b *testBuffer) Close() error {
	return nil
}

func TestWriteCSV(t *testing.T) {
	md := []string{
		"name=First, type=UTF8, encoding=PLAIN",
		"name=Middle, type=UTF8, encoding=PLAIN",
		"name=Last, type=UTF8, encoding=PLAIN",
		"name=BirthCity, type=UTF8, encoding=PLAIN",
	}

	buf := &testBuffer{}
	pw, err := NewCSVWriter(md, buf, 2)
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
	if err := buf.Close(); err != nil {
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
	buf := &testBuffer{}

	pw, err := NewCSVWriter(md, buf, 2)
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
	if err := buf.Close(); err != nil {
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
		buf := &testBuffer{}
		pw, err := NewCSVWriter(md, buf, 2)
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
		buf.Close()
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
		buf := &testBuffer{}
		pw, err := NewCSVWriter(md, buf, 2)
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
		buf.Close()
	}
}
