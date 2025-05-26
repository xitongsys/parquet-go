package local

import (
	"os"
)

type localFile struct {
	filePath string
	file     *os.File
}
