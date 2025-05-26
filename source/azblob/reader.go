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

// Compile time check that *azBlockBlob implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*azBlobReader)(nil)

// azBlobReader is ParquetFileReader for azblob
type azBlobReader struct {
	azBlockBlob
	fileSize int64
	offset   int64
}

var (
	errWhence        = errors.New("Seek: invalid whence")
	errInvalidOffset = errors.New("Seek: invalid offset")
	errReadNotOpened = errors.New("Read: url not opened")
)

// NewAzBlobFileReader creates an Azure Blob FileReader, to be used with NewParquetReader
func NewAzBlobFileReader(ctx context.Context, URL string, credential any, clientOptions blockblob.ClientOptions) (source.ParquetFileReader, error) {
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

	return NewAzBlobFileReaderWithClient(ctx, URL, client)
}

// NewAzBlobFileReaderWithClient creates an Azure Blob FileReader, to be used with NewParquetReader
func NewAzBlobFileReaderWithClient(ctx context.Context, URL string, client *blockblob.Client) (source.ParquetFileReader, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	file := &azBlobReader{
		azBlockBlob: azBlockBlob{
			ctx:             ctx,
			blockBlobClient: client,
		},
	}

	return file.Open(URL)
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *azBlobReader) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errWhence
	}

	switch whence {
	case io.SeekStart:
		// we are at the position right no
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
func (s *azBlobReader) Read(p []byte) (n int, err error) {
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

// Close is a no-op for reader
func (s *azBlobReader) Close() error {
	return nil
}

// Open creates a new block blob to perform reads
func (s *azBlobReader) Open(URL string) (source.ParquetFileReader, error) {
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
	props, err := s.blockBlobClient.GetProperties(s.ctx, nil)
	if err != nil {
		return nil, err
	}
	fileSize := *props.ContentLength

	pf := &azBlobReader{
		azBlockBlob: azBlockBlob{
			ctx:             s.ctx,
			url:             u,
			blockBlobClient: s.blockBlobClient,
		},
		fileSize: fileSize,
	}

	return pf, nil
}
