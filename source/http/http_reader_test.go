package http

import (
	"errors"
	"strings"
	"testing"
)

func Test_http_reader_no_range_support(t *testing.T) {
	testCases := []struct {
		URL           string
		expectedError error
	}{
		{"https://no-such-host.tld/", errors.New("no such host")},
		{"https://www.google.com/", errors.New("does not support range")},
		{"https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet", nil},
	}

	for _, tc := range testCases {
		_, err := NewHttpReader(tc.URL, false, false, map[string]string{})
		if tc.expectedError == nil {
			if err != nil {
				t.Errorf("expected nil error but got [%v]", err)
			}
		} else {
			if err == nil {
				t.Errorf("expected error like [%v] but got nil", tc.expectedError)
			} else if !strings.Contains(err.Error(), tc.expectedError.Error()) {
				t.Errorf("expected error like [%v] but got [%v]", tc.expectedError, err)
			}
		}
	}
}
