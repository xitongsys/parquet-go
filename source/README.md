# parquet-go-source 

parquet-go-source is a source provider for parquet-go. Your source must implement ParquetFile interface:

```go
type ParquetFile interface {
	io.Seeker
	io.Reader
	io.Writer
	io.Closer
	Open(name string) (ParquetFile, error)
	Create(name string) (ParquetFile, error)
}
```

Now it supports:
* Local
* HDFS
* S3 (by [shsing2000](https://github.com/shsing2000))
* GCS (by [AOHUA](https://github.com/AOHUA))
* MemoryFileSystem (by [daikokoro](https://github.com/daidokoro))
* MemoryBuffer (by [pmalekn](https://github.com/pmalekn))
* HTTP Multipart Request Body (by [mcgrawia](https://github.com/mcgrawia))

Thanks for all the contributors !
