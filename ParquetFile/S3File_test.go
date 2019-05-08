package ParquetFile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/mock/gomock"
	"github.com/xitongsys/parquet-go/mocks"
)

func TestNewS3FileWriterPreparesAnUploader(t *testing.T) {

}

func TestNewS3FileReaderDeterminesFileSize(t *testing.T) {

}

func TestSeek(t *testing.T) {
	testcases := []struct {
		name           string
		filesize       int64
		currentOffset  int64
		offset         int64
		whence         int
		expectedOffset int64
		expectedError  error
	}{
		{"no file size seek start", 0, 500, 5, io.SeekStart, 5, nil},
		{"no file size seek current", 0, 500, 5, io.SeekCurrent, 5, nil},
		{"no file size seek end", 0, 500, -8, io.SeekEnd, -8, nil},
		{"seek start", 20, 10, 5, io.SeekStart, 5, nil},
		{"seek start read past end", 20, 0, 21, io.SeekStart, 0, errInvalidOffset},
		{"seek current", 20, 5, 5, io.SeekCurrent, 10, nil},
		{"seek current read past end", 20, 10, 20, io.SeekCurrent, 0, errInvalidOffset},
		{"seek end", 20, 10, -5, io.SeekEnd, -5, nil},
		{"seek end read past beginning", 20, 0, -30, io.SeekEnd, 0, errInvalidOffset},
		{"invalid whence", 20, 0, 0, 6, 0, errWhence},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s := &S3File{
				fileSize: tc.filesize,
				offset:   tc.currentOffset,
				whence:   tc.whence,
			}

			offset, err := s.Seek(tc.offset, tc.whence)
			if offset != tc.expectedOffset {
				t.Errorf("expected offset to be %d but got %d", tc.expectedOffset, offset)
			}
			if err != tc.expectedError {
				t.Errorf("expected error to be %v but got %v", tc.expectedError, err)
			}
		})
	}
}

func TestReadBeyondEOF(t *testing.T) {
	// file is at the end already
	s := &S3File{
		fileSize: 10,
		offset:   10,
	}

	b := make([]byte, 10)
	readBytes, err := s.Read(b)
	if readBytes != 0 {
		t.Errorf("expected to read 0 bytes but got %d", readBytes)
	}

	if err != io.EOF {
		t.Errorf("expected error %q but got %q", io.EOF.Error(), err.Error())
	}
}

func TestReadBodyLargerThanProvidedBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	buf := bytes.NewBufferString("some body data that is larger than expected")
	bufReadCloser := ioutil.NopCloser(buf)
	mockClient := mocks.NewMockS3API(ctrl)
	mockClient.EXPECT().GetObjectWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{Body: bufReadCloser}, nil
		})
	s := &S3File{
		fileSize:   100,
		offset:     10,
		downloader: s3manager.NewDownloaderWithClient(mockClient),
	}

	b := make([]byte, 4)
	readBytes, err := s.Read(b)
	if readBytes != len(b) {
		t.Errorf("expected to read %d bytes but got %d", len(b), readBytes)
	}

	if err != nil {
		t.Errorf("expected error to be nil but got %q", err.Error())
	}
}

func TestReadDownloadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errMessage := "some download error"
	buf := bytes.NewBufferString("some data")
	bufReadCloser := ioutil.NopCloser(buf)
	mockClient := mocks.NewMockS3API(ctrl)
	mockClient.EXPECT().GetObjectWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{Body: bufReadCloser}, errors.New(errMessage)
		})
	s := &S3File{
		fileSize:   100,
		offset:     10,
		downloader: s3manager.NewDownloaderWithClient(mockClient),
	}

	b := make([]byte, 4)
	readBytes, err := s.Read(b)
	if readBytes != 0 {
		t.Errorf("expected to read 0 bytes but got %d", readBytes)
	}

	if err.Error() != errMessage {
		t.Errorf("expected error to be %q but got %q", errMessage, err.Error())
	}
}

func TestRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	data := "some data"
	buf := bytes.NewBufferString(data)
	bufReadCloser := ioutil.NopCloser(buf)
	mockClient := mocks.NewMockS3API(ctrl)
	mockClient.EXPECT().GetObjectWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{Body: bufReadCloser}, nil
		})
	s := &S3File{
		fileSize:   100,
		offset:     0,
		downloader: s3manager.NewDownloaderWithClient(mockClient),
	}

	b := make([]byte, 9)
	readBytes, err := s.Read(b)
	if readBytes != len(data) {
		t.Errorf("expected to read %d bytes but got %d", buf.Cap(), readBytes)
	}

	if err != nil {
		t.Errorf("expected error to be nil but got %q", err.Error())
	}

	if string(b) != data {
		t.Errorf("expected data to be %q but got %q", data, string(b))
	}
}

func TestWriteWithUninitializedUploader(t *testing.T) {

}

func TestWriteWithPriorEncounteredError(t *testing.T) {
	data := []byte("some data")
	errMessage := "some write error"
	s := &S3File{
		writeOpened: true,
		err:         errors.New(errMessage),
	}

	writtenBytes, err := s.Write(data)
	if writtenBytes != 0 {
		t.Errorf("expected number of byte written to be 0 but got %d", writtenBytes)
	}

	if err.Error() != errMessage {
		t.Errorf("expected error to be %q but got %q", errMessage, err.Error())
	}
}

func TestWriteError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errMessage := "some write error"
	data := []byte("some data")
	bucket := "test-bucket"
	key := "test/foobar.parquet"

	buf := bytes.NewBuffer(data)
	req, err := http.NewRequest(http.MethodPost, "http://localhost/upload", buf)
	if err != nil {
		t.Error("unable to create mock S3 client http request")
	}
	mockClient := mocks.NewMockS3API(ctrl)
	mockClient.EXPECT().PutObjectRequest(gomock.Any()).
		Return(
			&request.Request{
				HTTPRequest: req,
				Error:       errors.New(errMessage),
			},
			&s3.PutObjectOutput{})

	s := &S3File{
		ctx:        context.Background(),
		BucketName: bucket,
		Key:        key,
		client:     mockClient,
		writeDone:  make(chan error),
	}

	s.openWrite()
	writtenBytes, err := s.Write(data)
	if writtenBytes != len(data) {
		t.Errorf("expected number of byte written to be %d but got %d", len(data), writtenBytes)
	}

	if err != nil {
		t.Errorf("expected error to be nil but got %q", err.Error())
	}

	err = s.Close()
	if err.Error() != errMessage {
		t.Errorf("expected error to be %q but got %q", errMessage, err.Error())
	}
}

func TestWrite(t *testing.T) {

}

func TestCloseWaitForUploadCompletion(t *testing.T) {

}

func TestClose(t *testing.T) {

}

func TestOpen(t *testing.T) {

}

func TestCreate(t *testing.T) {

}

func TestOpenWriteUploadFailuresPreventFurtherWrites(t *testing.T) {
	// ctrl := gomock.NewController(t)
	// defer ctrl.Finish()

	// message := []byte("secret message")
	// errMessage := "some writer error"
	// bucket := "test-bucket"
	// key := "test/foobar.parquet"

	// mockClient := mocks.NewMockS3API(ctrl)
	// // mockClient.EXPECT().PutObjectRequest(gomock.Any()).Return(&request.Request{}, nil)
	// s := &S3File{
	// 	BucketName: bucket,
	// 	Key:        key,
	// 	client:     mockClient,
	// }

	// s.openWrite()
	// s.pipeWriter.Write(message)
	// // s.pipeWriter.CloseWithError(errors.New(errMessage))

	// // wait for err

	// select {
	// case <-time.After(1 * time.Second):
	// 	t.Error("expected error to be set")
	// case err := <-s.writeDone:
	// 	if err.Error() != errMessage {
	// 		t.Errorf("expected error to be %q but got %q", errMessage, err.Error())
	// 	}
	// }

	// s.Close()
}

func TestOpenWrite(t *testing.T) {
	// ctrl := gomock.NewController(t)
	// defer ctrl.Finish()

	// errMessage := "some client error"
	// bucket := "test-bucket"
	// key := "test/foobar.parquet"

	// mockClient := mocks.NewMockS3API(ctrl)
	// s := &S3File{
	// 	BucketName: bucket,
	// 	Key:        key,
	// 	client:     mockClient,
	// }

	// s.openWrite()

}

func TestOpenReadFileSizeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errMessage := "some client error"
	bucket := "test-bucket"
	key := "test/foobar.parquet"

	mockClient := mocks.NewMockS3API(ctrl)
	mockClient.EXPECT().HeadObject(gomock.Any()).
		DoAndReturn(func(hoi *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			if *hoi.Bucket != bucket {
				t.Errorf("expected bucket %q but got %q", bucket, *hoi.Bucket)
			}

			if *hoi.Key != key {
				t.Errorf("expected key %q but got %q", bucket, *hoi.Key)
			}

			return nil, errors.New(errMessage)
		})

	s := &S3File{
		BucketName: bucket,
		Key:        key,
		client:     mockClient,
	}

	err := s.openRead()
	if err.Error() != errMessage {
		t.Errorf("expected error %s but got %s", errMessage, err.Error())
	}
}

func TestOpenRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bucket := "test-bucket"
	key := "test/foobar.parquet"
	filesize := int64(123)

	mockClient := mocks.NewMockS3API(ctrl)
	mockClient.EXPECT().HeadObject(gomock.Any()).
		DoAndReturn(func(hoi *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			if *hoi.Bucket != bucket {
				t.Errorf("expected bucket %q but got %q", bucket, *hoi.Bucket)
			}

			if *hoi.Key != key {
				t.Errorf("expected key %q but got %q", bucket, *hoi.Key)
			}

			return &s3.HeadObjectOutput{ContentLength: aws.Int64(filesize)}, nil
		})

	s := &S3File{
		BucketName: bucket,
		Key:        key,
		client:     mockClient,
	}

	err := s.openRead()
	if err != nil {
		t.Errorf("expected error to be nil but got %s", err.Error())
	}

	if !s.readOpened {
		t.Errorf("expected readOpened to be %t but got %t", true, s.readOpened)
	}

	if s.fileSize != filesize {
		t.Errorf("expected filesize to be %d but got %d", filesize, s.fileSize)
	}
}

func TestGetBytesRange(t *testing.T) {
	testcases := []struct {
		name     string
		filesize int64
		offset   int64
		whence   int
		length   int
		expected string
	}{
		{"no file size seek start", 0, 5, io.SeekStart, 10, "bytes=5-14"},
		{"no file size seek current", 0, 5, io.SeekCurrent, 10, "bytes=5-14"},
		{"no file size seek end", 0, -8, io.SeekEnd, 10, "bytes=-8"},
		{"no file size invalid whence", 0, 0, 6, 10, ""},
		{"seek start", 20, 0, io.SeekStart, 10, "bytes=0-9"},
		{"seek start read past end", 20, 0, io.SeekStart, 30, "bytes=0-19"},
		{"seek current", 20, 5, io.SeekCurrent, 10, "bytes=5-14"},
		{"seek current read past end", 20, 10, io.SeekCurrent, 20, "bytes=10-19"},
		{"seek end", 20, -5, io.SeekEnd, 5, "bytes=15-19"},
		{"seek end buffer larger than requested", 20, -5, io.SeekEnd, 10, "bytes=15-19"},
		{"seek end read past beginning", 20, -30, io.SeekEnd, 10, "bytes=0-9"},
		{"invalid whence", 20, 0, 6, 10, ""},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s := &S3File{
				fileSize: tc.filesize,
				offset:   tc.offset,
				whence:   tc.whence,
			}

			rangeHeader := s.getBytesRange(tc.length)
			if rangeHeader != tc.expected {
				t.Errorf("expected byte range header %q but got %q", tc.expected, rangeHeader)
			}
		})
	}
}
