package swiftsource

import (
	"github.com/ncw/swift"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *swiftFile implement the source.ParquetFileReader and source.ParquetFileWriter interface.
var (
	_ source.ParquetFileReader = (*swiftFile)(nil)
	_ source.ParquetFileWriter = (*swiftFile)(nil)
)

type swiftFile struct {
	Connection *swift.Connection

	Container string
	FilePath  string

	FileReader *swift.ObjectOpenFile
	FileWriter *swift.ObjectCreateFile
}

func newSwiftFile(containerName, filePath string, conn *swift.Connection) *swiftFile {
	return &swiftFile{
		Connection: conn,
		Container:  containerName,
		FilePath:   filePath,
	}
}

func NewSwiftFileReader(container, filePath string, conn *swift.Connection) (source.ParquetFileReader, error) {
	res := newSwiftFile(container, filePath, conn)
	return res.Open(filePath)
}

func NewSwiftFileWriter(container, filePath string, conn *swift.Connection) (source.ParquetFileWriter, error) {
	res := newSwiftFile(container, filePath, conn)
	return res.Create(filePath)
}

func (file *swiftFile) Open(name string) (source.ParquetFileReader, error) {
	if name == "" {
		name = file.FilePath
	}

	fr, _, err := file.Connection.ObjectOpen(file.Container, name, false, nil)
	if err != nil {
		return nil, err
	}

	res := &swiftFile{
		Connection: file.Connection,
		Container:  file.Container,
		FilePath:   name,
		FileReader: fr,
	}

	return res, nil
}

func (file *swiftFile) Create(name string) (source.ParquetFileWriter, error) {
	if name == "" {
		name = file.FilePath
	}

	fw, err := file.Connection.ObjectCreate(file.Container, name, false, "", "", nil)
	if err != nil {
		return nil, err
	}

	res := &swiftFile{
		Connection: file.Connection,
		Container:  file.Container,
		FilePath:   name,
		FileWriter: fw,
	}

	return res, nil
}

func (file *swiftFile) Read(b []byte) (n int, err error) {
	return file.FileReader.Read(b)
}

func (file *swiftFile) Seek(offset int64, whence int) (int64, error) {
	return file.FileReader.Seek(offset, whence)
}

func (file *swiftFile) Write(p []byte) (n int, err error) {
	return file.FileWriter.Write(p)
}

func (file *swiftFile) Close() error {
	if file.FileWriter != nil {
		if err := file.FileWriter.Close(); err != nil {
			return err
		}
	}
	if file.FileReader != nil {
		if err := file.FileReader.Close(); err != nil {
			return err
		}
	}
	return nil
}
