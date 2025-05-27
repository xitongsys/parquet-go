package s3v2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hangxie/parquet-go/v2/source"
)

type s3ReadClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// Compile time check that *s3File implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*s3Reader)(nil)

type s3Reader struct {
	s3File
	client         s3ReadClient
	readOpened     bool
	fileSize       int64
	offset         int64
	whence         int
	version        *string
	socket         io.ReadCloser
	minRequestSize int64
}

const (
	rangeHeader           = "bytes=%d-%d"
	rangeHeaderSuffix     = "bytes=%d"
	defaultMinRequestSize = math.MaxUint32
)

var (
	errWhence        = errors.New("Seek: invalid whence")
	errInvalidOffset = errors.New("Seek: invalid offset")
)

// NewS3FileReader creates an S3 FileReader, to be used with NewParquetReader
func NewS3FileReader(ctx context.Context, bucket, key string, version *string, cfgs ...*aws.Config) (source.ParquetFileReader, error) {
	return NewS3FileReaderWithParams(ctx, S3FileReaderParams{
		Bucket:  bucket,
		Key:     key,
		Version: version,
	})
}

// NewS3FileReaderWithClient is the same as NewS3FileReader but allows passing
// your own S3 client
func NewS3FileReaderWithClient(ctx context.Context, s3Client s3ReadClient, bucket, key string, version *string) (source.ParquetFileReader, error) {
	return NewS3FileReaderWithParams(ctx, S3FileReaderParams{
		Bucket:   bucket,
		Key:      key,
		Version:  version,
		S3Client: s3Client,
	})
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *s3Reader) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errWhence
	}

	if s.fileSize > 0 {
		switch whence {
		case io.SeekStart:
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekCurrent:
			offset += s.offset
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekEnd:
			if offset == 0 {
				offset = s.fileSize
			} else if offset > 0 || -offset > s.fileSize {
				return 0, errInvalidOffset
			}
		}
	}

	s.offset = offset
	s.whence = whence

	s.closeSocket()
	return s.offset, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *s3Reader) Read(p []byte) (n int, err error) {
	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, io.EOF
	}

	defer func() {
		if err != nil {
			s.closeSocket()
		}
	}()

	if s.socket == nil {
		err = s.openSocket(int64(len(p)))
		if err != nil {
			return 0, err
		}
	}
	n, err = s.socket.Read(p)
	// Because the chunk size is not infinite, we might hit the end of the socket while
	// there's still data in the file. In this case, we close the socket so that the next
	// read call will request a new one, and we return a nil error so that the caller
	// will not think the file is done.
	if err == io.EOF {
		err = nil
		s.closeSocket()
	}
	s.offset += int64(n)

	return n, err
}

// openSocket issues a new GetObject request to retrieve the next chunk of data from the
// object.
func (s *s3Reader) openSocket(numBytes int64) error {
	if numBytes < s.minRequestSize {
		numBytes = s.minRequestSize
	}
	getObjRange := s.getBytesRange(numBytes)
	getObj := &s3.GetObjectInput{
		Bucket:    aws.String(s.bucketName),
		Key:       aws.String(s.key),
		VersionId: s.version,
	}
	if len(getObjRange) > 0 {
		getObj.Range = aws.String(getObjRange)
	}

	out, err := s.client.GetObject(s.ctx, getObj)
	if err != nil {
		return err
	}
	s.socket = out.Body
	return nil
}

func (s *s3Reader) closeSocket() {
	if s.socket != nil {
		_ = s.socket.Close()
		s.socket = nil
	}
}

// Close signals write completion and cleans up any
// open streams. Will block until pending uploads are complete.
func (s *s3Reader) Close() error {
	s.closeSocket()
	return nil
}

// Open creates a new S3 File instance to perform concurrent reads
func (s *s3Reader) Open(name string) (source.ParquetFileReader, error) {
	s.lock.RLock()
	readOpened := s.readOpened
	s.lock.RUnlock()
	if !readOpened {
		if err := s.openRead(); err != nil {
			return nil, err
		}
	}

	// ColumBuffer passes in an empty string for name
	if len(name) == 0 {
		name = s.key
	}

	// create a new instance
	pf := &s3Reader{
		s3File: s3File{
			ctx:        s.ctx,
			bucketName: s.bucketName,
			key:        name,
		},
		client:         s.client,
		version:        s.version,
		readOpened:     s.readOpened,
		fileSize:       s.fileSize,
		minRequestSize: s.minRequestSize,
		offset:         0,
	}
	return pf, nil
}

func (s *s3Reader) Clone() (source.ParquetFileReader, error) {
	return NewS3FileReaderWithClient(s.ctx, s.client, s.bucketName, s.key, s.version)
}

// openRead verifies the requested file is accessible and
// tracks the file size
func (s *s3Reader) openRead() error {
	hoi := &s3.HeadObjectInput{
		Bucket:    aws.String(s.bucketName),
		Key:       aws.String(s.key),
		VersionId: s.version,
	}

	hoo, err := s.client.HeadObject(s.ctx, hoi)
	if err != nil {
		return err
	}

	s.lock.Lock()
	s.readOpened = true
	if hoo.ContentLength != nil && *hoo.ContentLength != 0 {
		s.fileSize = *hoo.ContentLength
	}
	s.lock.Unlock()

	return nil
}

// getBytesRange returns the range request header string
func (s *s3Reader) getBytesRange(numBytes int64) string {
	var (
		byteRange string
		begin     int64
		end       int64
	)

	// Processing for unknown file size relies on the requester to
	// know which ranges are valid. May occur if caller is missing HEAD permissions.
	if s.fileSize < 1 {
		switch s.whence {
		case io.SeekStart, io.SeekCurrent:
			byteRange = fmt.Sprintf(rangeHeader, s.offset, s.offset+int64(numBytes)-1)
		case io.SeekEnd:
			byteRange = fmt.Sprintf(rangeHeaderSuffix, s.offset)
		}
		return byteRange
	}

	switch s.whence {
	case io.SeekStart, io.SeekCurrent:
		begin = s.offset
	case io.SeekEnd:
		begin = s.fileSize + s.offset
	default:
		return byteRange
	}

	endIndex := s.fileSize - 1
	if begin < 0 {
		begin = 0
	}
	end = begin + int64(numBytes) - 1
	if end > endIndex {
		end = endIndex
	}

	byteRange = fmt.Sprintf(rangeHeader, begin, end)
	return byteRange
}

// S3FileReaderParams contains fields used to initialize and configure an s3Reader object
// for reading.
type S3FileReaderParams struct {
	Bucket string
	Key    string

	// S3Client will be used to issue requests to S3. If not set, a new one will be
	// created. Optional.
	S3Client s3ReadClient
	// Version is the version of the S3 object that will be read. If not set, the newest
	// version will be read. Optional.
	Version *string
	// MinRequestSize controls the amount of data per request that the S3File will ask for
	// from S3. Optional.
	// A large MinRequestSize should improve performance and reduce AWS costs due to
	// number of request. However, in some cases it may increase AWS costs due to data
	// processing or data transfer. For best results, set it at or above the largest of
	// the footer size and the biggest chunk size in the parquet file.
	// S3File will not buffer a large amount of data in memory at one time, regardless
	// of the value of MinRequestSize.
	MinRequestSize int
}
