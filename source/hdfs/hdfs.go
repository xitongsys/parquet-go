package hdfs

import (
	"github.com/colinmarc/hdfs/v2"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *hdfsFile implement the source.ParquetFileReader and source.ParquetFileWriter interface.
var (
	_ source.ParquetFileReader = (*hdfsFile)(nil)
	_ source.ParquetFileWriter = (*hdfsFile)(nil)
)

type hdfsFile struct {
	hosts []string
	user  string

	client     *hdfs.Client
	filePath   string
	fileReader *hdfs.FileReader
	fileWriter *hdfs.FileWriter
}

func NewHdfsFileWriter(hosts []string, user, name string) (source.ParquetFileWriter, error) {
	res := &hdfsFile{
		hosts:    hosts,
		user:     user,
		filePath: name,
	}
	return res.Create(name)
}

func NewHdfsFileReader(hosts []string, user, name string) (source.ParquetFileReader, error) {
	res := &hdfsFile{
		hosts:    hosts,
		user:     user,
		filePath: name,
	}
	return res.Open(name)
}

func (f *hdfsFile) Create(name string) (source.ParquetFileWriter, error) {
	var err error
	hf := new(hdfsFile)
	hf.hosts = f.hosts
	hf.user = f.user
	hf.client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hf.hosts,
		User:      hf.user,
	})
	hf.filePath = name
	if err != nil {
		return hf, err
	}
	hf.fileWriter, err = hf.client.Create(name)
	return hf, err
}

func (f *hdfsFile) Open(name string) (source.ParquetFileReader, error) {
	var err error
	if name == "" {
		name = f.filePath
	}

	hf := new(hdfsFile)
	hf.hosts = f.hosts
	hf.user = f.user
	hf.client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hf.hosts,
		User:      hf.user,
	})
	hf.filePath = name
	if err != nil {
		return hf, err
	}
	hf.fileReader, err = hf.client.Open(name)
	return hf, err
}

func (f *hdfsFile) Seek(offset int64, pos int) (int64, error) {
	return f.fileReader.Seek(offset, pos)
}

func (f *hdfsFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = f.fileReader.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

func (f *hdfsFile) Write(b []byte) (n int, err error) {
	return f.fileWriter.Write(b)
}

func (f *hdfsFile) Close() error {
	if f.fileReader != nil {
		if err := f.fileReader.Close(); err != nil {
			return err
		}
	}
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
