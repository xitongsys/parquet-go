package hdfs

import (
	"github.com/colinmarc/hdfs/v2"
)

type hdfsFile struct {
	hosts    []string
	user     string
	filePath string
	client   *hdfs.Client
}
