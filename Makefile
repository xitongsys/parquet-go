PACKAGES=`go list ./... | grep -v example`

test:
	for pkg in ${PACKAGES};do \
		go test -v -cover $$pkg; \
		if [ "$$?" != "0" ];then exit 1; fi \
	done;
