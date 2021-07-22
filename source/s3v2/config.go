package s3v2

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

var (
	cfg      *aws.Config
	configMu sync.Mutex
)

// Config from shared config rather than explicit configuration
func getConfig() aws.Config {
	configMu.Lock()
	defer configMu.Unlock()

	if cfg != nil {
		return *cfg
	}
	c, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	cfg = &c
	return *cfg
}
