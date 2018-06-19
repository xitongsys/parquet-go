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
	res := &HdfsFile{
		Hosts:    hosts,
		User:     user,
		FilePath: name,
	}
	return res.Create(name)
}

func NewHdfsFileReader(hosts []string, user string, name string) (ParquetFile, error) {
	res := &HdfsFile{
		Hosts:    hosts,
		User:     user,
		FilePath: name,
	}
	return res.Open(name)
}

func (self *HdfsFile) Create(name string) (ParquetFile, error) {
	var err error
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
	hf.FileWriter, err = hf.Client.Create(name)
	return hf, err

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
func (self *HdfsFile) Seek(offset int64, pos int) (int64, error) {
	return self.FileReader.Seek(offset, pos)
}

func (self *HdfsFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = self.FileReader.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

func (self *HdfsFile) Write(b []byte) (n int, err error) {
	return self.FileWriter.Write(b)
}

func (self *HdfsFile) Close() error {
	if self.FileReader != nil {
		self.FileReader.Close()
	}
	if self.FileWriter != nil {
		self.FileWriter.Close()
	}
	if self.Client != nil {
		self.Client.Close()
	}
	return nil
}
