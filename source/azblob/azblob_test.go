package azblob

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type testCase struct {
	url string
	err error
}

var testCases []testCase = []testCase{
	{
		// Public accessible data: https://azure.microsoft.com/en-us/services/open-datasets/catalog/
		url: "https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_zip/year=2010/part-00178-tid-5434563040420806442-84b5e4ab-8ab1-4e28-beb1-81caf32ca312-1919656.c000.snappy.parquet",
		err: nil,
	},
	{
		url: "https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_zip/",
		err: errors.New("RESPONSE ERROR (ErrorCode=BlobNotFound)"),
	},
	{
		// the idea is that Azure blob does now allow "-" in storage account name so there should be no such a storage account
		url: "https://non-existent.blob.core.windows.net/container/blob",
		err: errors.New("no such host"),
	},
}

func TestOpen_NewAzBlobFileReader(t *testing.T) {
	for _, tc := range testCases {
		_, err := NewAzBlobFileReader(context.Background(), tc.url, nil, azblob.ClientOptions{
			Retry: policy.RetryOptions{
				TryTimeout: 10 * time.Second,
				MaxRetries: 1,
			},
		})
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

func TestOpen_NewAzBlobFileReaderWithSharedKey(t *testing.T) {
	for _, tc := range testCases {
		_, err := NewAzBlobFileReaderWithSharedKey(context.Background(), tc.url, nil, azblob.ClientOptions{
			Retry: policy.RetryOptions{
				TryTimeout: 10 * time.Second,
				MaxRetries: 1,
			},
		})
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

func TestOpen_NewAzBlobFileReaderWithClient(t *testing.T) {
	for _, tc := range testCases {
		testClient, _ := azblob.NewBlockBlobClientWithNoCredential(tc.url, &azblob.ClientOptions{})
		_, err := NewAzBlobFileReaderWithClient(context.Background(), tc.url, testClient)
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
	_, err := NewAzBlobFileReaderWithClient(context.Background(), "dummy-url", nil)
	expected := "client cannot be nil"
	if err == nil {
		t.Errorf("expected [%s] but got: <nil>", expected)
	} else if !strings.Contains(err.Error(), expected) {
		t.Errorf("expected [%s] error but got: %s", expected, err.Error())
	}
}
