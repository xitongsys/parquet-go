package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/bobg/gcsobj"
	"github.com/xitongsys/parquet-go/source"
)

// Compile time check that *File implement the source.ParquetFile interface.
var _ source.ParquetFile = (*File)(nil)

// File represents a File that can be read from or written to.
type File struct {
	ProjectID  string
	BucketName string
	FilePath   string

	gcsClient      *storage.Client
	gcsReader      *gcsobj.Reader
	gcsWriter      *storage.Writer
	object         *storage.ObjectHandle
	ctx            context.Context //nolint:containedctx // Needed to create new readers and writers
	externalClient bool
}

// NewGcsFileWriter will create a new GCS file writer.
func NewGcsFileWriter(ctx context.Context, projectID, bucketName, name string) (*File, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	r, err := NewGcsFileWriterWithClient(ctx, client, projectID, bucketName, name)
	if err != nil {
		return nil, err
	}

	// Set externalClient to false so we close it when calling `Close`.
	r.externalClient = false

	return r, nil
}

// NewGcsFileWriter will create a new GCS file writer with the passed client.
func NewGcsFileWriterWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string) (*File, error) {
	obj := client.Bucket(bucketName).Object(name)

	// Close writer to flush changes and force the file to be created
	writer := obj.NewWriter(ctx)
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return NewGcsFileReaderWithClient(ctx, client, projectID, bucketName, name)
}

// NewGcsFileReader will create a new GCS file reader.
func NewGcsFileReader(ctx context.Context, projectID, bucketName, name string) (*File, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	r, err := NewGcsFileReaderWithClient(ctx, client, projectID, bucketName, name)
	if err != nil {
		return nil, err
	}

	// Set externalClient to false so we close it when calling `Close`.
	r.externalClient = false

	return r, nil
}

// NewGcsFileReader will create a new GCS file reader with the passed client.
func NewGcsFileReaderWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string) (*File, error) {
	obj := client.Bucket(bucketName).Object(name)

	reader, err := gcsobj.NewReader(ctx, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to create new reader: %w", err)
	}

	return &File{
		ProjectID:      projectID,
		BucketName:     bucketName,
		FilePath:       name,
		gcsClient:      client,
		gcsReader:      reader,
		object:         obj,
		ctx:            ctx,
		externalClient: true,
	}, nil
}

// Open will create a new GCS file reader/writer and open the object named as
// the passed named. If name is left empty the same object as currently opened
// will be re-opened.
func (g *File) Open(name string) (source.ParquetFile, error) {
	if name == "" {
		name = g.FilePath
	}

	if g.gcsClient == nil {
		return NewGcsFileReader(g.ctx, g.ProjectID, g.BucketName, name)
	}

	return NewGcsFileReaderWithClient(g.ctx, g.gcsClient, g.ProjectID, g.BucketName, name)
}

// Create will create a new GCS file reader/writer and open the object named as
// the passed named. If name is left empty the same object as currently opened
// will be re-opened.
func (g *File) Create(name string) (source.ParquetFile, error) {
	if name == "" {
		name = g.FilePath
	}

	if g.gcsClient == nil {
		return NewGcsFileWriter(g.ctx, g.ProjectID, g.BucketName, name)
	}

	return NewGcsFileWriterWithClient(g.ctx, g.gcsClient, g.ProjectID, g.BucketName, name)
}

// Seek implements io.Seeker.
func (g *File) Seek(offset int64, whence int) (int64, error) {
	return g.gcsReader.Seek(offset, whence)
}

// Read implements io.Reader.
func (g *File) Read(b []byte) (int, error) {
	return g.gcsReader.Read(b)
}

// Write implements io.Writer.
func (g *File) Write(b []byte) (int, error) {
	if g.gcsWriter == nil {
		g.gcsWriter = g.object.NewWriter(g.ctx)
	}

	return g.gcsWriter.Write(b)
}

// Close implements io.Closer.
func (g *File) Close() error {
	if !g.externalClient && g.gcsClient != nil {
		if err := g.gcsClient.Close(); err != nil {
			return err
		}

		g.gcsClient = nil
	}

	if g.gcsWriter != nil {
		if err := g.gcsWriter.Close(); err != nil {
			return err
		}

		g.gcsWriter = nil
	}

	return g.gcsReader.Close()
}
