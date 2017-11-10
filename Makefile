PACKAGES=`go list ./... | grep -v example`

test:
	for pkg in ${PACKAGES};do \
		go test -v -cover $$pkg; \
	done;
