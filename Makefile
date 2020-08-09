PACKAGES=`go list ./... | grep -v example`

.PHONEY: test
test:
	go test -v -cover -coverprofile=cover.out ${PACKAGES}

