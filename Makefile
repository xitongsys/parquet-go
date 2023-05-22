PACKAGES=`go list ./... | grep -v example`

test:
	go test --count 1 -v -cover ${PACKAGES}

format:
	go fmt github.com/xitongsys/parquet-go/...

.PHONEY: test
