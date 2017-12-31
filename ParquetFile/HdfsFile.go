package ParquetFile

import (
	"github.com/colinmarc/hdfs"
)

type HdfsFile struct {
	Hosts []string
	User  string

	Client     *hdfs.Client
	FilePath   string
	FileReader *hdfs.FileReader
	FileWriter *hdfs.FileWriter
}

func NewHdfsFileWriter(hosts []string, user string, name string) (ParquetFile, error) {
	var err error
	res := &HdfsFile{
		Hosts:    hosts,
		User:     user,
		FilePath: name,
	}

	res.Client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hosts,
		User:      user,
	})
	if err != nil {
		return nil, err
	}

	res.FileWriter, err = res.Client.Create(name)
	return res, err
}

func NewHdfsFileReader(hosts []string, user string, name string) (ParquetFile, error) {
	res := &HdfsFile{
		Hosts:    hosts,
		User:     user,
		FilePath: name,
	}
	return res.Open(name)
}

func (self *HdfsFile) Open(name string) (ParquetFile, error) {
	var (
		err error
	)
	if name == "" {
		name = self.FilePath
	}

	hf := new(HdfsFile)
	hf.Hosts = self.Hosts
	hf.User = self.User
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
func (self *HdfsFile) Seek(offset int, pos int) (int64, error) {
	return self.FileReader.Seek(int64(offset), pos)
}

func (self *HdfsFile) Read(b []byte) (n int, err error) {
	return self.FileReader.Read(b)
}

func (self *HdfsFile) Write(b []byte) (n int, err error) {
	return self.FileWriter.Write(b)
}

func (self *HdfsFile) Close() {
	if self.FileReader != nil {
		self.FileReader.Close()
	}
	if self.FileWriter != nil {
		self.FileWriter.Close()
	}
	if self.Client != nil {
		self.Client.Close()
	}
}
