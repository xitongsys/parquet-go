package azblob

import (
	"context"
	"errors"
	"io"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/xitongsys/parquet-go/source"
)

// AzBlockBlob is ParquetFile for azblob
type AzBlockBlob struct {
	ctx          context.Context
	URL          *url.URL
	credential   azblob.Credential
	blockBlobURL *azblob.BlockBlobURL

	// write-related fields
	writeDone     chan error
	pipeReader    *io.PipeReader
	pipeWriter    *io.PipeWriter
	writerOptions WriterOptions

	// read-related fields
	fileSize      int64
	offset        int64
	readerOptions ReaderOptions
}

var (
	errWhence         = errors.New("Seek: invalid whence")
	errInvalidOffset  = errors.New("Seek: invalid offset")
	errReadNotOpened  = errors.New("Read: url not opened")
	errWriteNotOpened = errors.New("Write url not opened")
)

// ReaderOptions is used to configure azblob read behavior, including HTTP, retry, and logging settings
type ReaderOptions struct {
	// HTTPSender configures the sender of HTTP requests
	HTTPSender pipeline.Factory
	// Retry configures the built-in retry policy behavior.
	RetryOptions azblob.RetryOptions
	// Log configures the pipeline's logging infrastructure indicating what information is logged and where.
	Log pipeline.LogOptions
}

// WriterOptions is used to configure azblob write behavior, including HTTP, retry, and logging settings
type WriterOptions struct {
	// HTTPSender configures the sender of HTTP requests
	HTTPSender pipeline.Factory
	// Retry configures the built-in retry policy behavior.
	RetryOptions azblob.RetryOptions
	// Log configures the pipeline's logging infrastructure indicating what information is logged and where.
	Log pipeline.LogOptions
	// Parallelism limits the number of go routines created to read blob content (0 = default)
	Parallelism int
}

// NewAzBlobFileWriter creates an Azure Blob FileWriter, to be used with NewParquetWriter
func NewAzBlobFileWriter(ctx context.Context, URL string, credential azblob.Credential, options WriterOptions) (source.ParquetFile, error) {
	file := &AzBlockBlob{
		ctx:           ctx,
		credential:    credential,
		writerOptions: options,
	}

	return file.Create(URL)
}

// NewAzBlobFileReader creates an Azure Blob FileReader, to be used with NewParquetReader
func NewAzBlobFileReader(ctx context.Context, URL string, credential azblob.Credential, options ReaderOptions) (source.ParquetFile, error) {
	file := &AzBlockBlob{
		ctx:           ctx,
		credential:    credential,
		readerOptions: options,
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
	if s.blockBlobURL == nil {
		return 0, errReadNotOpened
	}

	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, io.EOF
	}

	count := int64(len(p))
	resp, err := s.blockBlobURL.Download(s.ctx, s.offset, count, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return 0, err
	}
	if s.fileSize < 0 {
		s.fileSize = resp.ContentLength()
	}

	toRead := s.fileSize - s.offset
	if toRead > count {
		toRead = count
	}

	body := resp.Body(azblob.RetryReaderOptions{})
	bytesRead, err := io.ReadFull(body, p[:toRead])
	if err != nil {
		return 0, err
	}

	s.offset += int64(bytesRead)

	return bytesRead, nil
}

// Write len(p) bytes from p
func (s *AzBlockBlob) Write(p []byte) (n int, err error) {
	if s.blockBlobURL == nil {
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

	blobURL := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(s.credential, azblob.PipelineOptions{HTTPSender: s.readerOptions.HTTPSender, Retry: s.readerOptions.RetryOptions, Log: s.readerOptions.Log}))

	fileSize := int64(-1)
	props, err := blobURL.GetProperties(s.ctx, azblob.BlobAccessConditions{})
	if err != nil {
		return &AzBlockBlob{}, err
	}
	fileSize = props.ContentLength()

	pf := &AzBlockBlob{
		ctx:           s.ctx,
		URL:           u,
		credential:    s.credential,
		blockBlobURL:  &blobURL,
		fileSize:      fileSize,
		readerOptions: s.readerOptions,
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

	blobURL := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(s.credential, azblob.PipelineOptions{HTTPSender: s.writerOptions.HTTPSender, Retry: s.writerOptions.RetryOptions, Log: s.writerOptions.Log}))

	// get account properties to validate credentials
	if _, err := blobURL.GetAccountInfo(s.ctx); err != nil {
		return nil, err
	}

	pf := &AzBlockBlob{
		ctx:           s.ctx,
		URL:           u,
		credential:    s.credential,
		blockBlobURL:  &blobURL,
		writerOptions: s.writerOptions,
		writeDone:     make(chan error),
	}

	pf.pipeReader, pf.pipeWriter = io.Pipe()

	go func(ctx context.Context, blobURL *azblob.BlockBlobURL, o WriterOptions, reader io.Reader, readerPipeSource *io.PipeWriter, done chan error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := azblob.UploadStreamToBlockBlob(ctx, reader, *blobURL, azblob.UploadStreamToBlockBlobOptions{MaxBuffers: o.Parallelism})
		if err != nil {
			readerPipeSource.CloseWithError(err)
		}

		done <- err
	}(pf.ctx, pf.blockBlobURL, pf.writerOptions, pf.pipeReader, pf.pipeWriter, pf.writeDone)

	return pf, nil
}
