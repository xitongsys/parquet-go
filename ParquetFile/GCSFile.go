package ParquetFile

import (
	"context"

	"cloud.google.com/go/storage"
)

type GcsFile struct {
	ProjectId  string
	BucketName string
	Ctx        context.Context

	Client     *storage.Client
	Bucket     *storage.BucketHandle
	FilePath   string
	FileReader *storage.Reader
	FileWriter *storage.Writer
}

func NewGcsFileWriter(projectId string, bucketName string, name string, ctx context.Context) (ParquetFile, error) {
	res := &GcsFile{
		ProjectId:  projectId,
		BucketName: bucketName,
		Ctx:        ctx,
		FilePath:   name,
	}
	return res.Create(name)
}

func NewGcsFileReader(hosts []string, user string, name string) (ParquetFile, error) {
	res := &GcsFile{
		FilePath: name,
	}
	return res.Open(name)
}

func (self *GcsFile) Create(name string) (ParquetFile, error) {
	var err error
	gcs := new(GcsFile)
	gcs.Client, err = storage.NewClient(self.Ctx)
	gcs.FilePath = name
	if err != nil {
		return gcs, err
	}
	// must use existing bucket
	gcs.Bucket = gcs.Client.Bucket(self.BucketName)
	obj := gcs.Bucket.Object(name)
	gcs.FileWriter = obj.NewWriter(self.Ctx)
	return gcs, err

}
func (self *GcsFile) Open(name string) (ParquetFile, error) {
	var err error
	gcs := new(GcsFile)
	gcs.Client, err = storage.NewClient(self.Ctx)
	gcs.FilePath = name
	if err != nil {
		return gcs, err
	}
	// must use existing bucket
	gcs.Bucket = gcs.Client.Bucket(self.BucketName)
	obj := gcs.Bucket.Object(name)
	gcs.FileReader, err = obj.NewReader(self.Ctx)
	return gcs, err
}
func (self *GcsFile) Seek(offset int, pos int) (int64, error) {
	//Not implemented
	return 0, nil
}

func (self *GcsFile) Read(b []byte) (n int, err error) {
	return self.FileReader.Read(b)
}

func (self *GcsFile) Write(b []byte) (n int, err error) {
	return self.FileWriter.Write(b)
}

func (self *GcsFile) Close() {
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
