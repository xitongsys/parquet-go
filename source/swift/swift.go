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
	connection *swift.Connection

	container string
	filePath  string

	fileReader *swift.ObjectOpenFile
	fileWriter *swift.ObjectCreateFile
}

func newSwiftFile(containerName, filePath string, conn *swift.Connection) *swiftFile {
	return &swiftFile{
		connection: conn,
		container:  containerName,
		filePath:   filePath,
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
		name = file.filePath
	}

	fr, _, err := file.connection.ObjectOpen(file.container, name, false, nil)
	if err != nil {
		return nil, err
	}

	res := &swiftFile{
		connection: file.connection,
		container:  file.container,
		filePath:   name,
		fileReader: fr,
	}

	return res, nil
}

func (file *swiftFile) Create(name string) (source.ParquetFileWriter, error) {
	if name == "" {
		name = file.filePath
	}

	fw, err := file.connection.ObjectCreate(file.container, name, false, "", "", nil)
	if err != nil {
		return nil, err
	}

	res := &swiftFile{
		connection: file.connection,
		container:  file.container,
		filePath:   name,
		fileWriter: fw,
	}

	return res, nil
}

func (file *swiftFile) Read(b []byte) (n int, err error) {
	return file.fileReader.Read(b)
}

func (file *swiftFile) Seek(offset int64, whence int) (int64, error) {
	return file.fileReader.Seek(offset, whence)
}

func (file *swiftFile) Write(p []byte) (n int, err error) {
	return file.fileWriter.Write(p)
}

func (file *swiftFile) Close() error {
	if file.fileWriter != nil {
		if err := file.fileWriter.Close(); err != nil {
			return err
		}
	}
	if file.fileReader != nil {
		if err := file.fileReader.Close(); err != nil {
			return err
		}
	}
	return nil
}
