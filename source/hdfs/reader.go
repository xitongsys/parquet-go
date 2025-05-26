package hdfs

import (
	"github.com/colinmarc/hdfs/v2"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *hdfsFile implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*hdfsReader)(nil)

type hdfsReader struct {
	hdfsFile
	fileReader *hdfs.FileReader
}

func NewHdfsFileReader(hosts []string, user, name string) (source.ParquetFileReader, error) {
	res := &hdfsReader{
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

	return res.Open(name)
}

func (f *hdfsReader) Open(name string) (source.ParquetFileReader, error) {
	var err error
	f.fileReader, err = f.client.Open(name)
	return f, err
}

func (f *hdfsReader) Seek(offset int64, pos int) (int64, error) {
	return f.fileReader.Seek(offset, pos)
}

func (f *hdfsReader) Read(b []byte) (int, error) {
	var cnt, n int
	var err error
	for cnt < len(b) {
		n, err = f.fileReader.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

func (f *hdfsReader) Close() error {
	if f.fileReader != nil {
		if err := f.fileReader.Close(); err != nil {
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
