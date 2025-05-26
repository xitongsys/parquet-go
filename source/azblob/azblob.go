package azblob

import (
	"context"
	"net/url"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

type azBlockBlob struct {
	ctx             context.Context
	url             *url.URL
	blockBlobClient *blockblob.Client
}
