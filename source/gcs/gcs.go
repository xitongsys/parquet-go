package gcs

import (
	"context"
	"errors"
	"io"

	"cloud.google.com/go/storage"
	"github.com/xitongsys/parquet-go/source"
)

var (
	errWhence        = errors.New("Seek: invalid whence")
	errInvalidOffset = errors.New("Seek: invalid offset")
)

type GcsFile struct {
	ProjectId  string
	BucketName string
	Ctx        context.Context

	Client         *storage.Client
	externalClient bool
	Bucket         *storage.BucketHandle
	FilePath       string
	FileReader     *storage.Reader
	FileWriter     *storage.Writer

	offset   int64
	whence   int
	fileSize int64
}

func NewGcsFileWriter(ctx context.Context, projectId string, bucketName string, name string) (source.ParquetFile, error) {
	res := &GcsFile{
		ProjectId:  projectId,
		BucketName: bucketName,
		Ctx:        ctx,
		FilePath:   name,
	}
	return res.Create(name)
}

func NewGcsFileWriterWithClient(ctx context.Context, client *storage.Client, projectId string, bucketName string, name string) (source.ParquetFile, error) {
	res := &GcsFile{
		ProjectId:      projectId,
		BucketName:     bucketName,
		Ctx:            ctx,
		Client:         client,
		externalClient: true,
		FilePath:       name,
	}
	return res.Create(name)
}

func NewGcsFileReader(ctx context.Context, projectId string, bucketName string, name string) (source.ParquetFile, error) {
	res := &GcsFile{
		ProjectId:  projectId,
		BucketName: bucketName,
		Ctx:        ctx,
		FilePath:   name,
	}
	return res.Open(name)
}

func NewGcsFileReaderWithClient(ctx context.Context, client *storage.Client, projectId string, bucketName string, name string) (source.ParquetFile, error) {
	res := &GcsFile{
		ProjectId:      projectId,
		BucketName:     bucketName,
		Ctx:            ctx,
		Client:         client,
		externalClient: true,
		FilePath:       name,
	}
	return res.Open(name)
}

func (self *GcsFile) Create(name string) (source.ParquetFile, error) {
	var err error
	gcs := new(GcsFile)
	if self.Client == nil {
		gcs.Client, err = storage.NewClient(self.Ctx)
		gcs.externalClient = false
	} else {
		gcs.Client = self.Client
		gcs.externalClient = self.externalClient
	}
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

func (self *GcsFile) Open(name string) (source.ParquetFile, error) {
	var err error
	gcs := new(GcsFile)
	if self.Client == nil {
		gcs.Client, err = storage.NewClient(self.Ctx)
		gcs.externalClient = false
	} else {
		gcs.Client = self.Client
		gcs.externalClient = self.externalClient
	}
	if err != nil {
		return gcs, err
	}
	if name == "" {
		gcs.FilePath = self.FilePath
	} else {
		gcs.FilePath = name
	}
	// must use existing bucket
	gcs.Bucket = gcs.Client.Bucket(self.BucketName)
	obj := gcs.Bucket.Object(gcs.FilePath)
	attrs, err := obj.Attrs(self.Ctx)
	if err != nil {
		return gcs, err
	}
	gcs.fileSize = attrs.Size
	gcs.Ctx = self.Ctx
	gcs.ProjectId = self.ProjectId
	gcs.BucketName = self.BucketName
	return gcs, err
}

func (self *GcsFile) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errWhence
	}

	if self.fileSize > 0 {
		switch whence {
		case io.SeekStart:
			if offset < 0 || offset > self.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekCurrent:
			offset += self.offset
			if offset < 0 || offset > self.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekEnd:
			if offset > -1 || -offset > self.fileSize {
				return 0, errInvalidOffset
			}
		}
	}

	self.offset = offset
	self.whence = whence
	return self.offset, nil
}

func (self *GcsFile) Read(b []byte) (cnt int, err error) {
	if self.fileSize > 0 && self.offset >= self.fileSize {
		return 0, io.EOF
	}

	ln := len(b)

	obj := self.Bucket.Object(self.FilePath)
	if self.offset == 0 {
		self.FileReader, err = obj.NewReader(self.Ctx)
	} else {
		var length int64
		if self.offset < 0 || (self.whence == io.SeekEnd && int64(ln) >= self.fileSize-self.offset) {
			length = -1
		} else {
			length = int64(ln)
		}
		self.FileReader, err = obj.NewRangeReader(self.Ctx, self.offset, length)
		if err != nil {
			return
		}
	}
	defer self.FileReader.Close()

	var n int
	for cnt < ln {
		n, err = self.FileReader.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	self.offset += int64(cnt)
	return cnt, err
}

func (self *GcsFile) Write(b []byte) (n int, err error) {
	return self.FileWriter.Write(b)
}

func (self *GcsFile) Close() error {
	if self.FileReader != nil {
		if err := self.FileReader.Close(); err != nil {
			return err
		}
	}
	if self.FileWriter != nil {
		if err := self.FileWriter.Close(); err != nil {
			return err
		}
	}
	if self.Client != nil && !self.externalClient {
		err := self.Client.Close()
		self.Client = nil
		if err != nil {
			return err
		}
	}
	return nil
}
