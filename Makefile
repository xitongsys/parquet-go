.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

BUILD_TIME  = $(shell date +%FT%T%z)
BUILD_DIR   = $(CURDIR)/build
GIT_HASH    = $(shell git rev-parse --short HEAD)
PKG_PREFIX  = github.com/xitongsys/parquet-go
VERSION     = $(shell git describe --tags --always)

# go option
CGO_ENABLED := 0
GO          ?= go
GOBIN       = $(shell go env GOPATH)/bin
GOFLAGS     := -trimpath
GOSOURCES   := $(shell find . -type f -name '*.go')
LDFLAGS     := -w -s

.EXPORT_ALL_VARIABLES:

.PHONY: all
all: deps tools format lint test build  ## Build all common targets

.PHONY: format
format: tools  ## Format all go code
	@echo "==> Formatting all go code"
	@$(GOBIN)/gofumpt -w -extra $(GOSOURCES)
	@$(GOBIN)/goimports -w -local $(PKG_PREFIX) $(GOSOURCES)

.PHONY: lint
lint: tools  ## Run static code analysis
	@echo "==> Running static code analysis"
	@$(GOBIN)/golangci-lint cache clean
	@$(GOBIN)/golangci-lint run ./... \
		--timeout 5m \
		--exclude-use-default=false
	@$(GOBIN)/gocyclo -over 15 . > /tmp/gocyclo.output; \
		if [[ -s /tmp/gocyclo.output ]]; then \
			echo functions with gocyclo score higher than 15; \
			cat /tmp/gocyclo.output | sed 's/^/    /'; \
			false; \
		fi

.PHONY: deps
deps:  ## Install prerequisite for build
	@echo "==> Installing prerequisite for build"
	@go mod tidy

.PHONY: tools
tools:  ## Install build tools
	@echo "==> Installing build tools"
	@(cd /tmp; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
		go install github.com/jstemmer/go-junit-report/v2@latest; \
		go install mvdan.cc/gofumpt@latest; \
		go install github.com/fzipp/gocyclo/cmd/gocyclo@latest; \
		go install golang.org/x/tools/cmd/goimports@latest; \
	)

.PHONY: clean
clean:  ## Clean up the build dirs
	@echo "==> Cleaning up build dirs"
	@rm -rf $(BUILD_DIR) vendor .venv

.PHONY: test
test: deps tools  ## Run unit tests
	@echo "==> Running unit tests"
	@mkdir -p $(BUILD_DIR)/test
	@set -euo pipefail ; \
		cd $(BUILD_DIR)/test; \
		CGO_ENABLED=1 go test -v -race -count 1 -trimpath \
			-coverprofile=coverage.out $(CURDIR)/... \
			| tee go-test.output ; \
		go tool cover -html=coverage.out -o coverage.html ; \
		go tool cover -func=coverage.out -o coverage.txt ; \
		cat go-test.output | $(GOBIN)/go-junit-report > junit.xml ; \
		cat coverage.txt

.PHONY: help
help:  ## Print list of Makefile targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
