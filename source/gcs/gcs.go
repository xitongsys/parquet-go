package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/bobg/gcsobj"
	"github.com/xitongsys/parquet-go/source"
)

// Compile time check that *File implement the source.ParquetFile interface.
var _ source.ParquetFile = &File{}

// File represents a File that can be read from or written to.
type File struct {
	ProjectID  string
	BucketName string
	FilePath   string

	gcsReader *gcsobj.Reader
	gcsWriter *storage.Writer
	object    *storage.ObjectHandle
	ctx       context.Context //nolint:containedctx // Needed to create new readers and writers
}

// NewGcsFileWriter will create a new GCS file writer.
func NewGcsFileWriter(ctx context.Context, projectID, bucketName, name string) (*File, error) {
	return NewGcsFileReader(ctx, projectID, bucketName, name)
}

// NewGcsFileWriter will create a new GCS file writer with the passed client.
func NewGcsFileWriterWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string) (*File, error) {
	return NewGcsFileReaderWithClient(ctx, client, projectID, bucketName, name)
}

// NewGcsFileWriter will create a new GCS file reader.
func NewGcsFileReader(ctx context.Context, projectID, bucketName, name string) (*File, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	return NewGcsFileReaderWithClient(ctx, client, projectID, bucketName, name)
}

// NewGcsFileWriter will create a new GCS file reader with the passed client.
func NewGcsFileReaderWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string) (*File, error) {
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(name)

	reader, err := gcsobj.NewReader(ctx, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to create new reader: %w", err)
	}

	return &File{
		ProjectID:  projectID,
		BucketName: bucketName,
		FilePath:   name,
		gcsReader:  reader,
		object:     obj,
		ctx:        ctx,
	}, nil
}

// Open will create a new GCS file reader/writer and open the object named as
// the passed named. If name is left empty the same object as currently opened
// will be re-opened.
func (g *File) Open(name string) (source.ParquetFile, error) {
	if name == "" {
		name = g.FilePath
	}

	return NewGcsFileReader(g.ctx, g.ProjectID, g.BucketName, name)
}

// Create will create a new GCS file reader/writer and open the object named as
// the passed named. If name is left empty the same object as currently opened
// will be re-opened.
func (g *File) Create(name string) (source.ParquetFile, error) {
	if name == "" {
		name = g.FilePath
	}

	return NewGcsFileReader(g.ctx, g.ProjectID, g.BucketName, name)
}

// Seek implements io.Seeker.
func (g *File) Seek(offset int64, whence int) (int64, error) {
	return g.gcsReader.Seek(offset, whence)
}

// Read implements io.Reader.
func (g *File) Read(b []byte) (cnt int, err error) {
	return g.gcsReader.Read(b)
}

// Write implements io.Writer.
func (g *File) Write(b []byte) (n int, err error) {
	if g.gcsWriter == nil {
		g.gcsWriter = g.object.NewWriter(g.ctx)
	}

	return g.gcsWriter.Write(b)
}

// Close implements io.Closer.
func (g *File) Close() error {
	if g.gcsWriter != nil {
		if err := g.gcsWriter.Close(); err != nil {
			return err
		}
	}

	return g.gcsReader.Close()
}
