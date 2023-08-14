package azblob

import (
	"context"
	"errors"
	"io"
	"net/url"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/xitongsys/parquet-go/source"
)

// AzBlockBlob is ParquetFile for azblob
type AzBlockBlob struct {
	ctx             context.Context
	URL             *url.URL
	blockBlobClient *blockblob.Client

	// write-related fields
	writeDone  chan error
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter

	// read-related fields
	fileSize int64
	offset   int64
}

var (
	errWhence         = errors.New("Seek: invalid whence")
	errInvalidOffset  = errors.New("Seek: invalid offset")
	errReadNotOpened  = errors.New("Read: url not opened")
	errWriteNotOpened = errors.New("Write url not opened")
)

// NewAzBlobFileWriter creates an Azure Blob FileWriter, to be used with NewParquetWriter
func NewAzBlobFileWriter(ctx context.Context, URL string, credential azcore.TokenCredential, clientOptions blockblob.ClientOptions) (source.ParquetFile, error) {
	var err error
	var client *blockblob.Client
	if credential == nil {
		client, err = blockblob.NewClientWithNoCredential(URL, &clientOptions)
	} else {
		client, err = blockblob.NewClient(URL, credential, &clientOptions)
	}
	if err != nil {
		return nil, err
	}

	return NewAzBlobFileWriterWithClient(ctx, URL, client)
}

// NewAzBlobFileWriterWithSharedKey creates an Azure Blob FileWriter, to be used with NewParquetWriter
func NewAzBlobFileWriterWithSharedKey(ctx context.Context, URL string, credential *blob.SharedKeyCredential, clientOptions blockblob.ClientOptions) (source.ParquetFile, error) {
	var err error
	var client *blockblob.Client
	if credential == nil {
		client, err = blockblob.NewClientWithNoCredential(URL, &clientOptions)
	} else {
		client, err = blockblob.NewClientWithSharedKeyCredential(URL, credential, &clientOptions)
	}
	if err != nil {
		return nil, err
	}

	return NewAzBlobFileWriterWithClient(ctx, URL, client)
}

// NewAzBlobFileWriterWithClient creates an Azure Blob FileWriter, to be used with NewParquetWriter
func NewAzBlobFileWriterWithClient(ctx context.Context, URL string, client *blockblob.Client) (source.ParquetFile, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	file := &AzBlockBlob{
		ctx:             ctx,
		blockBlobClient: client,
	}

	return file.Create(URL)
}

// NewAzBlobFileReader creates an Azure Blob FileReader, to be used with NewParquetReader
func NewAzBlobFileReader(ctx context.Context, URL string, credential azcore.TokenCredential, clientOptions blockblob.ClientOptions) (source.ParquetFile, error) {
	var err error
	var client *blockblob.Client
	if credential == nil {
		client, err = blockblob.NewClientWithNoCredential(URL, &clientOptions)
	} else {
		client, err = blockblob.NewClient(URL, credential, &clientOptions)
	}
	if err != nil {
		return nil, err
	}

	return NewAzBlobFileReaderWithClient(ctx, URL, client)
}

// NewAzBlobFileReaderWithSharedKey creates an Azure Blob FileReader, to be used with NewParquetReader
func NewAzBlobFileReaderWithSharedKey(ctx context.Context, URL string, credential *blob.SharedKeyCredential, clientOptions blockblob.ClientOptions) (source.ParquetFile, error) {
	var err error
	var client *blockblob.Client
	if credential == nil {
		client, err = blockblob.NewClientWithNoCredential(URL, &clientOptions)
	} else {
		client, err = blockblob.NewClientWithSharedKeyCredential(URL, credential, &clientOptions)
	}
	if err != nil {
		return nil, err
	}

	return NewAzBlobFileReaderWithClient(ctx, URL, client)
}

// NewAzBlobFileReaderWithClient creates an Azure Blob FileReader, to be used with NewParquetReader
func NewAzBlobFileReaderWithClient(ctx context.Context, URL string, client *blockblob.Client) (source.ParquetFile, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	file := &AzBlockBlob{
		ctx:             ctx,
		blockBlobClient: client,
	}

	return file.Open(URL)
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *AzBlockBlob) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errWhence
	}

	switch whence {
	case io.SeekStart:
		offset = offset
	case io.SeekCurrent:
		offset = s.offset + offset
	case io.SeekEnd:
		offset = s.fileSize + offset
	}

	if offset < 0 || offset > s.fileSize {
		return 0, errInvalidOffset
	}

	s.offset = offset

	return s.offset, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *AzBlockBlob) Read(p []byte) (n int, err error) {
	if s.blockBlobClient == nil {
		return 0, errReadNotOpened
	}

	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, io.EOF
	}

	count := int64(len(p))
	resp, err := s.blockBlobClient.DownloadStream(s.ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: s.offset,
			Count:  count,
		},
	})
	if err != nil {
		return 0, err
	}
	if s.fileSize < 0 {
		s.fileSize = *resp.ContentLength
	}

	toRead := s.fileSize - s.offset
	if toRead > count {
		toRead = count
	}

	body := resp.Body
	bytesRead, err := io.ReadFull(body, p[:toRead])
	if err != nil {
		return 0, err
	}

	s.offset += int64(bytesRead)

	return bytesRead, nil
}

// Write len(p) bytes from p
func (s *AzBlockBlob) Write(p []byte) (n int, err error) {
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
func (s *AzBlockBlob) Close() error {
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

// Open creates a new block blob to perform reads
func (s *AzBlockBlob) Open(URL string) (source.ParquetFile, error) {
	var u *url.URL
	if len(URL) == 0 && s.URL != nil {
		// ColumnBuffer passes in an empty string for name
		u = s.URL
	} else {
		var err error
		if u, err = url.Parse(URL); err != nil {
			return s, err
		}
	}
	fileSize := int64(-1)
	props, err := s.blockBlobClient.GetProperties(s.ctx, nil)
	if err != nil {
		return &AzBlockBlob{}, err
	}
	fileSize = *props.ContentLength

	pf := &AzBlockBlob{
		ctx:             s.ctx,
		URL:             u,
		blockBlobClient: s.blockBlobClient,
		fileSize:        fileSize,
	}

	return pf, nil
}

// Create a new blob url to perform writes
func (s *AzBlockBlob) Create(URL string) (source.ParquetFile, error) {
	var u *url.URL
	if len(URL) == 0 && s.URL != nil {
		// ColumnBuffer passes in an empty string for name
		u = s.URL
	} else {
		var err error
		if u, err = url.Parse(URL); err != nil {
			return s, err
		}
	}

	pf := &AzBlockBlob{
		ctx:             s.ctx,
		URL:             u,
		blockBlobClient: s.blockBlobClient,
		writeDone:       make(chan error),
	}

	pf.pipeReader, pf.pipeWriter = io.Pipe()

	go func(ctx context.Context, blobURL *blockblob.Client, reader io.Reader, readerPipeSource *io.PipeWriter, done chan error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := blobURL.UploadStream(ctx, reader, &blockblob.UploadStreamOptions{})

		done <- err
	}(pf.ctx, pf.blockBlobClient, pf.pipeReader, pf.pipeWriter, pf.writeDone)

	return pf, nil
}
