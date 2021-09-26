package azblob

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

func TestOpen_permissoin(t *testing.T) {
	testCases := []struct {
		url string
		err error
	}{
		{
			// Public accessible data: https://azure.microsoft.com/en-us/services/open-datasets/catalog/
			url: "https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_zip/year=2010/part-00178-tid-5434563040420806442-84b5e4ab-8ab1-4e28-beb1-81caf32ca312-1919656.c000.snappy.parquet",
			err: nil,
		},
		{
			url: "https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_zip/",
			err: errors.New("The specified blob does not exist"),
		},
		{
			// the idea is that Azure blob does now allow "-" in storage account name so there should be no such a storage account
			url: "https://non-existent.blob.core.windows.net/container/blob",
			err: errors.New("no such host"),
		},
	}

	readerOptions := ReaderOptions{
		RetryOptions: azblob.RetryOptions{
			MaxTries:   1,
			TryTimeout: time.Second * 10,
		},
	}

	for _, tc := range testCases {
		_, err := NewAzBlobFileReader(context.Background(), tc.url, azblob.NewAnonymousCredential(), readerOptions)
		if tc.err == nil {
			if err != nil {
				t.Errorf("expected no error but got %s", err.Error())
			}
			continue
		}
		if err == nil {
			t.Errorf("expected [%s] error but got nil", tc.err.Error())
		} else if !strings.Contains(err.Error(), tc.err.Error()) {
			t.Errorf("expected [%s] error but got: %s", tc.err.Error(), err.Error())
		}
	}
}
