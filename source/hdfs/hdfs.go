package hdfs

import (
	"github.com/colinmarc/hdfs/v2"

	"github.com/hangxie/parquet-go/v2/source"
)

type HdfsFile struct {
	Hosts []string
	User  string

	Client     *hdfs.Client
	FilePath   string
	FileReader *hdfs.FileReader
	FileWriter *hdfs.FileWriter
}

func NewHdfsFileWriter(hosts []string, user, name string) (source.ParquetFileWriter, error) {
	res := &HdfsFile{
		Hosts:    hosts,
		User:     user,
		FilePath: name,
	}
	return res.Create(name)
}

func NewHdfsFileReader(hosts []string, user, name string) (source.ParquetFileReader, error) {
	res := &HdfsFile{
		Hosts:    hosts,
		User:     user,
		FilePath: name,
	}
	return res.Open(name)
}

func (f *HdfsFile) Create(name string) (source.ParquetFileWriter, error) {
	var err error
	hf := new(HdfsFile)
	hf.Hosts = f.Hosts
	hf.User = f.User
	hf.Client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hf.Hosts,
		User:      hf.User,
	})
	hf.FilePath = name
	if err != nil {
		return hf, err
	}
	hf.FileWriter, err = hf.Client.Create(name)
	return hf, err
}

func (f *HdfsFile) Open(name string) (source.ParquetFileReader, error) {
	var err error
	if name == "" {
		name = f.FilePath
	}

	hf := new(HdfsFile)
	hf.Hosts = f.Hosts
	hf.User = f.User
	hf.Client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hf.Hosts,
		User:      hf.User,
	})
	hf.FilePath = name
	if err != nil {
		return hf, err
	}
	hf.FileReader, err = hf.Client.Open(name)
	return hf, err
}

func (f *HdfsFile) Seek(offset int64, pos int) (int64, error) {
	return f.FileReader.Seek(offset, pos)
}

func (f *HdfsFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = f.FileReader.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

func (f *HdfsFile) Write(b []byte) (n int, err error) {
	return f.FileWriter.Write(b)
}

func (f *HdfsFile) Close() error {
	if f.FileReader != nil {
		if err := f.FileReader.Close(); err != nil {
			return err
		}
	}
	if f.FileWriter != nil {
		if err := f.FileWriter.Close(); err != nil {
			return err
		}
	}
	if f.Client != nil {
		if err := f.Client.Close(); err != nil {
			return err
		}
	}
	return nil
}
