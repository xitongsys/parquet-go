PACKAGES=`go list ./... | grep -v example`

test:
	go test -v -cover ${PACKAGES}

.PHONEY: test
