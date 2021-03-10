PACKAGES=`go list ./... | grep -v example`

test:
	go test -v -cover ${PACKAGES}

.PHONEY: test

format:
	go fmt github.com/xitongsys/parquet-go/...
