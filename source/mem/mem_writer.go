package mem

import (
	"io"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/hangxie/parquet-go/v2/source"
)

// desclare unexported in-memory file-system
var memFs afero.Fs

// SetInMemFileFs - overrides local in-memory fileSystem
// NOTE: this is set by NewMemFileWriter is created
// and memFs is still nil
func SetInMemFileFs(fs *afero.Fs) {
	memFs = *fs
}

// GetMemFileFs - returns the current memory file-system
// being used by ParquetFile
func GetMemFileFs() afero.Fs {
	return memFs
}

// OnCloseFunc function type, handles what to do
// after converted file is closed in-memory.
// Close() will pass the filename string and data as io.reader
type OnCloseFunc func(string, io.Reader) error

type memFile struct {
	filePath string
	file     afero.File
	onClose  OnCloseFunc
}

// Compile time check that *memFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*memWriter)(nil)

// memWriter - ParquetFileWriter type for in-memory file operations
type memWriter struct {
	memFile
}

// NewMemFileWriter - intiates and creates an instance of MemFiles
// NOTE: there is no NewMemFileReader as this particular type was written
// to handle in-memory conversions and offloading. The results of
// conversion can then be stored and read via HDFS, LocalFS, etc without
// the need for loading the file back into memory directly
func NewMemFileWriter(name string, f OnCloseFunc) (source.ParquetFileWriter, error) {
	if memFs == nil {
		memFs = afero.NewMemMapFs()
	}

	var m memWriter
	m.onClose = f
	return m.Create(name)
}

// Create - create in-memory file
func (fs *memWriter) Create(name string) (source.ParquetFileWriter, error) {
	file, err := memFs.Create(name)
	if err != nil {
		return fs, err
	}

	fs.file = file
	fs.filePath = name
	return fs, nil
}

// Write - write file in-memory
func (fs *memWriter) Write(b []byte) (n int, err error) {
	return fs.file.Write(b)
}

// Close - close file and execute OnCloseFunc
func (fs *memWriter) Close() error {
	if err := fs.file.Close(); err != nil {
		return err
	}
	if fs.onClose != nil {
		f, _ := memFs.Open(fs.filePath)
		if err := fs.onClose(filepath.Base(fs.filePath), f); err != nil {
			return err
		}
	}
	return nil
}
