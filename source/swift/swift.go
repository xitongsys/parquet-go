package swiftsource

import (
	"github.com/ncw/swift"
	"github.com/xitongsys/parquet-go/source"
)

type SwiftFile struct {
	Connection *swift.Connection

	Container string
	FilePath  string

	FileReader *swift.ObjectOpenFile
	FileWriter *swift.ObjectCreateFile
}

func newSwiftFile(containerName string, filePath string, conn *swift.Connection) *SwiftFile {
	return &SwiftFile{
		Connection: conn,
		Container:  containerName,
		FilePath:   filePath,
	}
}

func NewSwiftFileReader(container string, filePath string, conn *swift.Connection) (source.ParquetFile, error) {
	res := newSwiftFile(container, filePath, conn)
	return res.Open(filePath)
}

func NewSwiftFileWriter(container string, filePath string, conn *swift.Connection) (source.ParquetFile, error) {
	res := newSwiftFile(container, filePath, conn)
	return res.Create(filePath)
}

func (file *SwiftFile) Open(name string) (source.ParquetFile, error) {
	if name == "" {
		name = file.FilePath
	}

	fr, _, err := file.Connection.ObjectOpen(file.Container, name, false, nil)
	if err != nil {
		return nil, err
	}

	res := &SwiftFile{
		Connection: file.Connection,
		Container:  file.Container,
		FilePath:   name,
		FileReader: fr,
	}

	return res, nil
}

func (file *SwiftFile) Create(name string) (source.ParquetFile, error) {
	if name == "" {
		name = file.FilePath
	}

	fw, err := file.Connection.ObjectCreate(file.Container, name, false, "", "", nil)
	if err != nil {
		return nil, err
	}

	res := &SwiftFile{
		Connection: file.Connection,
		Container:  file.Container,
		FilePath:   name,
		FileWriter: fw,
	}

	return res, nil
}

func (file *SwiftFile) Read(b []byte) (n int, err error) {
	return file.FileReader.Read(b)
}

func (file *SwiftFile) Seek(offset int64, whence int) (int64, error) {
	return file.FileReader.Seek(offset, whence)
}

func (file *SwiftFile) Write(p []byte) (n int, err error) {
	return file.FileWriter.Write(p)
}

func (file *SwiftFile) Close() error {
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
