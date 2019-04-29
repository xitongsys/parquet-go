package ParquetFile

//go:generate mockgen -destination=../mocks/mock_s3.go -package=mocks github.com/aws/aws-sdk-go/service/s3/s3iface S3API

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// S3File is a TODO
type S3File struct {
	ctx    context.Context
	client s3iface.S3API
	offset int64

	BucketName string
	Key        string
}

var activeS3Session *session.Session

// NewS3File is TODO
func NewS3File(ctx context.Context, region string, bucket string, key string) (ParquetFile, error) {
	if activeS3Session == nil {
		activeS3Session = session.Must(session.NewSession())
	}

	file := &S3File{
		client:     s3.New(activeS3Session, &aws.Config{}),
		BucketName: bucket,
		Key:        key,
	}

	return file, nil
}

// Seek is TODO
func (s *S3File) Seek(offset int64, whence int) (int64, error) {
	// goi := &s3.GetObjectInput{
	// 	Bucket: s.BucketName,
	// 	Key:    s.Key,
	// }
	s.offset = offset
	return 0, nil
}

// Read is TODO
func (s *S3File) Read(p []byte) (n int, err error) {

	// 	req, resp := client.GetObjectRequest(params)

	// err := req.Send()
	// if err == nil { // resp is now filled
	//     fmt.Println(resp)
	// }

	return 0, nil
}

// Write is TODO
func (s *S3File) Write(p []byte) (n int, err error) {
	// svc := s3.New(session.New())
	// input := &s3.PutObjectInput{
	//     Body:    aws.ReadSeekCloser(strings.NewReader("c:\\HappyFace.jpg")),
	//     Bucket:  aws.String("examplebucket"),
	//     Key:     aws.String("HappyFace.jpg"),
	//     Tagging: aws.String("key1=value1&key2=value2"),
	// }

	// result, err := svc.PutObject(input)
	return 0, nil
}

// Close is TODO
func (s *S3File) Close() error {
	s.offset = 0
	return nil
}

// Open is TODO reader
func (s *S3File) Open(name string) (ParquetFile, error) {
	s.offset = 0
	return s, nil
}

// Create is TODO writer
func (s *S3File) Create(name string) (ParquetFile, error) {
	s.offset = 0
	return s, nil
}
