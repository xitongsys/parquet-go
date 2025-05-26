package hdfs

import (
	"github.com/colinmarc/hdfs/v2"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *hdfsFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*hdfsWriter)(nil)

type hdfsWriter struct {
	hdfsFile
	fileWriter *hdfs.FileWriter
}

func NewHdfsFileWriter(hosts []string, user, name string) (source.ParquetFileWriter, error) {
	res := &hdfsWriter{
		hdfsFile: hdfsFile{
			hosts:    hosts,
			user:     user,
			filePath: name,
		},
	}

	var err error
	res.client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hosts,
		User:      user,
	})
	if err != nil {
		return nil, err
	}

	return res.Create(name)
}

func (f *hdfsWriter) Create(name string) (source.ParquetFileWriter, error) {
	var err error
	f.fileWriter, err = f.client.Create(name)
	return f, err
}

func (f *hdfsWriter) Write(b []byte) (n int, err error) {
	return f.fileWriter.Write(b)
}

func (f *hdfsWriter) Close() error {
	if f.fileWriter != nil {
		if err := f.fileWriter.Close(); err != nil {
			return err
		}
	}
	if f.client != nil {
		if err := f.client.Close(); err != nil {
			return err
		}
	}
	return nil
}
