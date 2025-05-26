package http

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *httpReader implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*httpReader)(nil)

type httpReader struct {
	url          string
	size         int64
	offset       int64
	httpClient   *http.Client
	extraHeaders map[string]string

	dedicatedTransport bool
}

const (
	rangeHeader        = "Range"
	rangeFormat        = "bytes=%d-%d"
	contentRangeHeader = "Content-Range"
)

var defaultClient *http.Client

func SetDefaultClient(client *http.Client) {
	defaultClient = client
}

func NewHttpReader(uri string, dedicatedTransport, ignoreTLSError bool, extraHeaders map[string]string) (source.ParquetFileReader, error) {
	var client *http.Client
	if defaultClient != nil {
		client = defaultClient
	} else {
		// make sure remote support range
		transport := http.DefaultTransport
		if dedicatedTransport {
			transport = &http.Transport{}
		}
		transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: ignoreTLSError}
		client = &http.Client{Transport: transport}
	}

	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range extraHeaders {
		req.Header.Add(k, v)
	}
	req.Header.Add(rangeHeader, fmt.Sprintf(rangeFormat, 0, 0))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// retrieve size
	contentRange := resp.Header.Values(contentRangeHeader)
	if len(contentRange) == 0 {
		return nil, fmt.Errorf("remote [%s] does not support range", uri)
	}

	tmp := strings.Split(contentRange[0], "/")
	if len(tmp) != 2 {
		return nil, fmt.Errorf("%s format is unknown: %s", contentRangeHeader, contentRange[0])
	}

	size, err := strconv.ParseInt(tmp[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse data size from %s: %s", contentRangeHeader, contentRange[0])
	}

	return &httpReader{
		url:                uri,
		size:               size,
		offset:             0,
		httpClient:         client,
		extraHeaders:       extraHeaders,
		dedicatedTransport: dedicatedTransport,
	}, nil
}

func (r *httpReader) Open(_ string) (source.ParquetFileReader, error) {
	return NewHttpReader(
		r.url,
		r.dedicatedTransport,
		r.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify,
		r.extraHeaders,
	)
}

func (r httpReader) Clone() (source.ParquetFileReader, error) {
	return NewHttpReader(
		r.url,
		r.dedicatedTransport,
		r.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify,
		r.extraHeaders,
	)
}

func (r *httpReader) Seek(offset int64, pos int) (int64, error) {
	switch pos {
	case io.SeekStart:
		r.offset = offset
	case io.SeekCurrent:
		r.offset += offset
	case io.SeekEnd:
		r.offset = r.size + offset
	default:
		return 0, fmt.Errorf("unknown whence: %d", pos)
	}

	if r.offset < 0 {
		r.offset = 0
	} else if r.offset >= r.size {
		r.offset = r.size
	}

	return r.offset, nil
}

func (r *httpReader) Read(b []byte) (int, error) {
	req, err := http.NewRequest(http.MethodGet, r.url, nil)
	if err != nil {
		return 0, err
	}

	for k, v := range r.extraHeaders {
		req.Header.Add(k, v)
	}
	req.Header.Add(rangeHeader, fmt.Sprintf(rangeFormat, r.offset, r.offset+int64(len(b)-1)))
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	buf, err := io.ReadAll(resp.Body)
	bytesRead := len(buf)
	for i := 0; i < bytesRead; i++ {
		b[i] = buf[i]
	}

	r.offset += int64(bytesRead)
	if r.offset > r.size {
		r.offset = r.size
	}
	return bytesRead, err
}

func (r *httpReader) Close() error {
	return nil
}
