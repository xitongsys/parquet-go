package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *gcsFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*gcsFileWriter)(nil)

type gcsFileWriter struct {
	gcsFile
	gcsWriter *storage.Writer
}

// NewGcsFileWriter will create a new GCS file writer.
func NewGcsFileWriter(ctx context.Context, projectID, bucketName, name string) (*gcsFileWriter, error) {
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
func NewGcsFileWriterWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string) (*gcsFileWriter, error) {
	obj := client.Bucket(bucketName).Object(name)

	// Close writer to flush changes and force the file to be created
	writer := obj.NewWriter(ctx)
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return &gcsFileWriter{
		gcsFile: gcsFile{
			projectID:      projectID,
			bucketName:     bucketName,
			filePath:       name,
			gcsClient:      client,
			object:         obj,
			ctx:            ctx,
			externalClient: true,
		},
		gcsWriter: writer,
	}, nil
}

// Create will create a new GCS file writer and open the object named as the
// passed named. If name is left empty the same object as currently opened
// will be re-opened.
func (g *gcsFileWriter) Create(name string) (source.ParquetFileWriter, error) {
	if g.gcsClient == nil {
		return NewGcsFileWriter(g.ctx, g.projectID, g.bucketName, name)
	}

	return NewGcsFileWriterWithClient(g.ctx, g.gcsClient, g.projectID, g.bucketName, name)
}

// Write implements io.Writer.
func (g *gcsFileWriter) Write(b []byte) (int, error) {
	return g.gcsWriter.Write(b)
}

// Close implements io.Closer.
func (g *gcsFileWriter) Close() error {
	if !g.externalClient && g.gcsClient != nil {
		if err := g.gcsClient.Close(); err != nil {
			return err
		}

		g.gcsClient = nil
	}

	return g.gcsWriter.Close()
}
