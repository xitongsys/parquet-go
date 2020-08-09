PACKAGES=`go list ./... | grep -v example`

.PHONEY: test
test:
	go test -race -v -cover -coverprofile=cover.out ${PACKAGES}

.PHONEY: benchmark
benchmark:
	go test -bench . -v ./writer -benchmem -cpuprofile cpu.out -memprofile mem.out

