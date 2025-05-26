package azblob

import (
	"context"
	"errors"
	"io"
	"net/url"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *azBlockBlob implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*azBlobWriter)(nil)

// azBlobWriter is ParquetFileWriter for azblob
type azBlobWriter struct {
	azBlockBlob
	writeDone  chan error
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
}

var errWriteNotOpened = errors.New("Write url not opened")

// NewAzBlobFileWriter creates an Azure Blob FileWriter, to be used with NewParquetWriter
func NewAzBlobFileWriter(ctx context.Context, URL string, credential any, clientOptions blockblob.ClientOptions) (source.ParquetFileWriter, error) {
	var err error
	var client *blockblob.Client
	if credential == nil {
		client, err = blockblob.NewClientWithNoCredential(URL, &clientOptions)
	} else {
		switch v := credential.(type) {
		case azcore.TokenCredential:
			client, err = blockblob.NewClient(URL, v, &clientOptions)
		case *blob.SharedKeyCredential:
			client, err = blockblob.NewClientWithSharedKeyCredential(URL, v, &clientOptions)
		default:
			return nil, errors.New("invalid credential type")
		}
	}
	if err != nil {
		return nil, err
	}

	return NewAzBlobFileWriterWithClient(ctx, URL, client)
}

// NewAzBlobFileWriterWithClient creates an Azure Blob FileWriter, to be used with NewParquetWriter
func NewAzBlobFileWriterWithClient(ctx context.Context, URL string, client *blockblob.Client) (source.ParquetFileWriter, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	file := &azBlobWriter{
		azBlockBlob: azBlockBlob{
			ctx:             ctx,
			blockBlobClient: client,
		},
	}

	return file.Create(URL)
}

// Write len(p) bytes from p
func (s *azBlobWriter) Write(p []byte) (n int, err error) {
	if s.blockBlobClient == nil {
		return 0, errWriteNotOpened
	}

	bytesWritten, writeError := s.pipeWriter.Write(p)
	if writeError != nil {
		s.pipeWriter.CloseWithError(err)
		return 0, writeError
	}

	return bytesWritten, nil
}

// Close signals write completion and cleans up any
// open streams. Will block until pending uploads are complete.
func (s *azBlobWriter) Close() error {
	var err error

	if s.pipeWriter != nil {
		if err = s.pipeWriter.Close(); err != nil {
			return err
		}

		// wait for pending uploads
		err = <-s.writeDone
	}

	return err
}

// Create a new blob url to perform writes
func (s *azBlobWriter) Create(URL string) (source.ParquetFileWriter, error) {
	var u *url.URL
	if len(URL) == 0 && s.url != nil {
		// ColumnBuffer passes in an empty string for name
		u = s.url
	} else {
		var err error
		if u, err = url.Parse(URL); err != nil {
			return s, err
		}
	}

	pf := &azBlobWriter{
		azBlockBlob: azBlockBlob{
			ctx:             s.ctx,
			url:             u,
			blockBlobClient: s.blockBlobClient,
		},
		writeDone: make(chan error),
	}

	pf.pipeReader, pf.pipeWriter = io.Pipe()

	go func(ctx context.Context, blobURL *blockblob.Client, reader *io.PipeReader, done chan error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := blobURL.UploadStream(ctx, reader, &blockblob.UploadStreamOptions{})

		done <- err
	}(pf.ctx, pf.blockBlobClient, pf.pipeReader, pf.writeDone)

	return pf, nil
}
