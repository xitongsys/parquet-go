package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/bobg/gcsobj"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *gcsFile implement the source.ParquetFileReader and source.ParquetFileWriter interface.
var (
	_ source.ParquetFileReader = (*gcsFile)(nil)
	_ source.ParquetFileWriter = (*gcsFile)(nil)
)

// gcsFile represents a gcsFile that can be read from or written to.
type gcsFile struct {
	projectID  string
	bucketName string
	filePath   string

	gcsClient      *storage.Client
	gcsReader      *gcsobj.Reader
	gcsWriter      *storage.Writer
	object         *storage.ObjectHandle
	ctx            context.Context //nolint:containedctx // Needed to create new readers and writers
	externalClient bool
}

// NewGcsFileWriter will create a new GCS file writer.
func NewGcsFileWriter(ctx context.Context, projectID, bucketName, name string) (*gcsFile, error) {
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
func NewGcsFileWriterWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string) (*gcsFile, error) {
	obj := client.Bucket(bucketName).Object(name)

	// Close writer to flush changes and force the file to be created
	writer := obj.NewWriter(ctx)
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return NewGcsFileReaderWithClient(ctx, client, projectID, bucketName, name)
}

// NewGcsFileReader will create a new GCS file reader.
func NewGcsFileReader(ctx context.Context, projectID, bucketName, name string) (*gcsFile, error) {
	// according to https://github.com/googleapis/google-cloud-go/blob/main/storage/storage.go#L103, default generation is -1
	return NewGcsFileReaderWithGeneration(ctx, projectID, bucketName, name, -1)
}

// NewGcsFileReaderWithGeneration will create a new GCS file reader for the specific generation.
func NewGcsFileReaderWithGeneration(ctx context.Context, projectID, bucketName, name string, generation int64) (*gcsFile, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	r, err := NewGcsFileReaderWithClientAndGeneration(ctx, client, projectID, bucketName, name, generation)
	if err != nil {
		return nil, err
	}

	// Set externalClient to false so we close it when calling `Close`.
	r.externalClient = false

	return r, nil
}

// NewGcsFileReaderWithClient will create a new GCS file reader with the passed client.
func NewGcsFileReaderWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string) (*gcsFile, error) {
	// according to https://github.com/googleapis/google-cloud-go/blob/main/storage/storage.go#L103, default generation is -1
	return NewGcsFileReaderWithClientAndGeneration(ctx, client, projectID, bucketName, name, -1)
}

// NewGcsFileReaderWithClientAndGeneration will create a new GCS file reader with the passed client for the specific generation.
func NewGcsFileReaderWithClientAndGeneration(ctx context.Context, client *storage.Client, projectID, bucketName, name string, generation int64) (*gcsFile, error) {
	obj := client.Bucket(bucketName).Object(name).Generation(generation)

	reader, err := gcsobj.NewReader(ctx, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to create new reader: %w", err)
	}

	return &gcsFile{
		projectID:      projectID,
		bucketName:     bucketName,
		filePath:       name,
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
func (g *gcsFile) Open(name string) (source.ParquetFileReader, error) {
	if name == "" {
		name = g.filePath
	}

	if g.gcsClient == nil {
		return NewGcsFileReader(g.ctx, g.projectID, g.bucketName, name)
	}

	return NewGcsFileReaderWithClient(g.ctx, g.gcsClient, g.projectID, g.bucketName, name)
}

// Create will create a new GCS file reader/writer and open the object named as
// the passed named. If name is left empty the same object as currently opened
// will be re-opened.
func (g *gcsFile) Create(name string) (source.ParquetFileWriter, error) {
	if name == "" {
		name = g.filePath
	}

	if g.gcsClient == nil {
		return NewGcsFileWriter(g.ctx, g.projectID, g.bucketName, name)
	}

	return NewGcsFileWriterWithClient(g.ctx, g.gcsClient, g.projectID, g.bucketName, name)
}

// Seek implements io.Seeker.
func (g *gcsFile) Seek(offset int64, whence int) (int64, error) {
	return g.gcsReader.Seek(offset, whence)
}

// Read implements io.Reader.
func (g *gcsFile) Read(b []byte) (int, error) {
	return g.gcsReader.Read(b)
}

// Write implements io.Writer.
func (g *gcsFile) Write(b []byte) (int, error) {
	if g.gcsWriter == nil {
		g.gcsWriter = g.object.NewWriter(g.ctx)
	}

	return g.gcsWriter.Write(b)
}

// Close implements io.Closer.
func (g *gcsFile) Close() error {
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
