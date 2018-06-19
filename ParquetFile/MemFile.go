package ParquetFile

import (
	"io"
	"path/filepath"

	"github.com/spf13/afero"
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

// MemFile - ParquetFile type for in-memory file operations
type MemFile struct {
	FilePath string
	File     afero.File
	OnClose  OnCloseFunc
}

// NewMemFileWriter - intiates and creates an instance of MemFiles
// NOTE: there is no NewMemFileReader as this particular type was written
// to handle in-memory converstions and offloading. The results of
// conversion can then be stored and read via HDFS, LocalFS, etc without
// the need for loading the file back into memory directly
func NewMemFileWriter(name string, f OnCloseFunc) (ParquetFile, error) {
	if memFs == nil {
		memFs = afero.NewMemMapFs()
	}

	var m MemFile
	m.OnClose = f
	return m.Create(name)
}

// Create - create in-memory file
func (fs *MemFile) Create(name string) (ParquetFile, error) {
	file, err := memFs.Create(name)
	if err != nil {
		return fs, err
	}

	fs.File = file
	fs.FilePath = name
	return fs, nil
}

// Open - open file in-memory
func (fs *MemFile) Open(name string) (ParquetFile, error) {
	var (
		err error
	)
	if name == "" {
		name = fs.FilePath
	}

	fs.FilePath = name
	fs.File, err = memFs.Open(name)
	return fs, err
}

// Seek - seek function
func (fs *MemFile) Seek(offset int64, pos int) (int64, error) {
	return fs.File.Seek(offset, pos)
}

// Read - read file
func (fs *MemFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = fs.File.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

// Write - write file in-memory
func (fs *MemFile) Write(b []byte) (n int, err error) {
	return fs.File.Write(b)
}

// Close - close file and execute OnCloseFunc
func (fs *MemFile) Close() error {
	if err := fs.File.Close(); err != nil {
		return err
	}
	if fs.OnClose != nil {
		f, _ := fs.Open(fs.FilePath)
		if err := fs.OnClose(filepath.Base(fs.FilePath), f); err != nil {
			return err
		}
	}
	return nil
}
