package swiftsource

import (
	"github.com/ncw/swift"
)

type swiftFile struct {
	connection *swift.Connection
	container  string
	filePath   string
}
