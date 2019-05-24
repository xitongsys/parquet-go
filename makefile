PACKAGES=`go list ./... | grep -v example`

test:
	dep ensure
	for pkg in ${PACKAGES};do \
		go test -v -cover $$pkg; \
		if [ "$$?" != "0" ];then exit 1; fi \
	done;
