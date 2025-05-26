package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/bobg/gcsobj"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *gcsFile implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*gcsReader)(nil)

type gcsReader struct {
	gcsFile
	generation int64
	gcsReader  *gcsobj.Reader
}

// NewGcsFileReader will create a new GCS file reader for the specific generation.
func NewGcsFileReader(ctx context.Context, projectID, bucketName, name string, generation int64) (*gcsReader, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	r, err := NewGcsFileReaderWithClient(ctx, client, projectID, bucketName, name, generation)
	if err != nil {
		return nil, err
	}

	// Set externalClient to false so we close it when calling `Close`.
	r.externalClient = false

	return r, nil
}

// NewGcsFileReaderWithClient will create a new GCS file reader with the passed client for the specific generation.
func NewGcsFileReaderWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string, generation int64) (*gcsReader, error) {
	obj := client.Bucket(bucketName).Object(name).Generation(generation)

	reader, err := gcsobj.NewReader(ctx, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to create new reader: %w", err)
	}

	return &gcsReader{
		gcsFile: gcsFile{
			projectID:      projectID,
			bucketName:     bucketName,
			filePath:       name,
			gcsClient:      client,
			object:         obj,
			ctx:            ctx,
			externalClient: true,
		},
		gcsReader:  reader,
		generation: generation,
	}, nil
}

// Open will create a new GCS file reader and open the object named as the
// passed named. If name is left empty the same object as currently opened
// will be re-opened.
func (g *gcsReader) Open(name string) (source.ParquetFileReader, error) {
	if name == "" {
		name = g.filePath
	}

	if g.gcsClient == nil {
		return NewGcsFileReader(g.ctx, g.projectID, g.bucketName, name, -1)
	}

	return NewGcsFileReaderWithClient(g.ctx, g.gcsClient, g.projectID, g.bucketName, name, -1)
}

// Seek implements io.Seeker.
func (g *gcsReader) Seek(offset int64, whence int) (int64, error) {
	return g.gcsReader.Seek(offset, whence)
}

// Read implements io.Reader.
func (g *gcsReader) Read(b []byte) (int, error) {
	return g.gcsReader.Read(b)
}

// Close implements io.Closer.
func (g *gcsReader) Close() error {
	if !g.externalClient && g.gcsClient != nil {
		if err := g.gcsClient.Close(); err != nil {
			return err
		}

		g.gcsClient = nil
	}

	return g.gcsReader.Close()
}
