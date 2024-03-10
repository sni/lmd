#!/usr/bin/make -f

PROJECT=lmd
MAKE:=make
SHELL:=bash
GOVERSION:=$(shell \
    go version | \
    awk -F'go| ' '{ split($$5, a, /\./); printf ("%04d%04d", a[1], a[2]); exit; }' \
)
# also update README.md and .github/workflows/citest.yml when changing minumum version
MINGOVERSION:=00010021
MINGOVERSIONSTR:=1.21
BUILD:=$(shell git rev-parse --short HEAD)
# see https://github.com/go-modules-by-example/index/blob/master/010_tools/README.md
# and https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module
TOOLSFOLDER=$(shell pwd)/tools
export GOBIN := $(TOOLSFOLDER)
export PATH := $(GOBIN):$(PATH)

BUILD_FLAGS=-ldflags "-s -w -X main.Build=$(BUILD)"
TEST_FLAGS=-timeout=5m $(BUILD_FLAGS)
GO=go

all: build

CMDS = $(shell cd ./cmd && ls -1)

tools: | versioncheck
	set -e; for DEP in $(shell grep "_ " buildtools/tools.go | awk '{ print $$2 }' | grep -v go-spew); do \
		( cd buildtools && $(GO) install $$DEP@latest ) ; \
	done
	set -e; for DEP in $(shell grep "_ " buildtools/tools.go | awk '{ print $$2 }' | grep go-spew); do \
		( cd buildtools && $(GO) install $$DEP ) ; \
	done
	( cd buildtools && $(GO) mod tidy )

updatedeps: versioncheck
	$(MAKE) clean
	$(MAKE) tools
	$(GO) mod download
	set -e; for dir in $(shell ls -d1 pkg/*); do \
		( cd ./$$dir && $(GO) mod download ); \
		( cd ./$$dir && GOPROXY=direct $(GO) get -u ); \
		( cd ./$$dir && GOPROXY=direct $(GO) get -t -u ); \
	done
	$(GO) mod download
	$(MAKE) cleandeps

cleandeps:
	set -e; for dir in $(shell ls -d1 pkg/*); do \
		( cd ./$$dir && $(GO) mod tidy ); \
	done
	$(GO) mod tidy
	( cd buildtools && $(GO) mod tidy )

vendor: go.work
	$(GO) mod download
	$(GO) mod tidy
	GOWORK=off $(GO) mod vendor

go.work: pkg/*
	echo "go $(MINGOVERSIONSTR)" > go.work
	$(GO) work use . pkg/* buildtools/.

dump:
	if [ $(shell grep -r Dump ./cmd/*/*.go ./pkg/*/*.go | grep -v 'Data::Dumper' | grep -v 'httputil.Dump' | grep -v logThreadDump | grep -v dump.go | wc -l) -ne 0 ]; then \
		sed -i.bak -e 's/\/\/go:build.*/\/\/ :build with debug functions/' -e 's/\/\/ +build.*/\/\/ build with debug functions/' pkg/$(PROJECT)/dump.go; \
	else \
		sed -i.bak -e 's/\/\/ :build.*/\/\/go:build ignore/' -e 's/\/\/ build.*/\/\/ +build ignore/' pkg/$(PROJECT)/dump.go; \
	fi
	rm -f pkg/$(PROJECT)/dump.go.bak

build: vendor
	set -e; for CMD in $(CMDS); do \
		( cd ./cmd/$$CMD && $(GO) build $(BUILD_FLAGS) -o ../../$$CMD ) ; \
	done

# run build watch, ex. with tracing: make build-watch -- -vv
build-watch: vendor tools
	set -x ; ls pkg/*/*.go cmd/*/*.go lmd.ini | entr -sr "$(MAKE) build && ./lmd $(filter-out $@,$(MAKECMDGOALS)) $(shell echo $(filter-out --,$(MAKEFLAGS)) | tac -s " ")"

build-linux-amd64: vendor
	set -e; for CMD in $(CMDS); do \
		( cd ./cmd/$$CMD && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build $(BUILD_FLAGS) -o ../../$$CMD.linux.amd64 ) ; \
	done


test: dump vendor
	$(GO) test -short -v $(TEST_FLAGS) pkg/*
	if grep -Irn TODO: ./cmd/ ./pkg/;  then exit 1; fi
	if grep -Irn Dump ./cmd/ ./pkg/ | grep -v 'Data::Dumper' | grep -v 'httputil.Dump' | grep -v logThreadDump | grep -v dump.go; then exit 1; fi

# test with filter
testf: vendor
	$(GO) test -short -v $(TEST_FLAGS) pkg/* -run "$(filter-out $@,$(MAKECMDGOALS))" 2>&1 | grep -v "no test files" | grep -v "no tests to run" | grep -v "^PASS"

longtest: vendor
	$(GO) test -v $(TEST_FLAGS) pkg/*
	rm -f pkg/lmd/mock*.sock

citest: tools vendor
	rm -f pkg/lmd/mock*.sock
	#
	# Checking gofmt errors
	#
	if [ $$(gofmt -s -l ./cmd/ ./pkg/ | wc -l) -gt 0 ]; then \
		echo "found format errors in these files:"; \
		gofmt -s -l ./cmd/ ./pkg/ ; \
		exit 1; \
	fi
	#
	# Checking TODO items
	#
	if grep -Irn TODO: ./cmd/ ./pkg/ ; then exit 1; fi
	#
	# Checking remaining debug calls
	#
	if grep -Irn Dump ./cmd/ ./pkg/ | grep -v 'Data::Dumper' | grep -v 'httputil.Dump' | grep -v logThreadDump | grep -v dump.go; then exit 1; fi
	#
	# Run other subtests
	#
	$(MAKE) golangci
	-$(MAKE) govulncheck
	$(MAKE) fmt
	#
	# Normal test cases
	#
	$(MAKE) test
	#
	# Benchmark tests
	#
	$(MAKE) benchmark
	#
	# Race rondition tests
	#
	$(MAKE) racetest
	#
	# All CI tests successful
	#

benchmark:
	$(GO) test $(TEST_FLAGS) -v -bench=B\* -run=^$$ -benchmem ./pkg/*

racetest:
	$(GO) test -race -short $(TEST_FLAGS) -coverprofile=coverage.txt -covermode=atomic -gcflags "-d=checkptr=0" ./pkg/*

covertest:
	$(GO) test -v $(TEST_FLAGS) -coverprofile=cover.out ./pkg/*
	$(GO) tool cover -func=cover.out
	$(GO) tool cover -html=cover.out -o coverage.html

coverweb:
	$(GO) test -v $(TEST_FLAGS) -coverprofile=cover.out ./pkg/*
	$(GO) tool cover -html=cover.out

clean:
	set -e; for CMD in $(CMDS); do \
		rm -f ./cmd/$$CMD/$$CMD; \
	done
	rm -f $(CMDS)
	rm -f pkg/lmd/mock*.sock
	rm -rf go.work
	rm -rf go.work.sum
	rm -f cover.out
	rm -f coverage.html
	rm -f coverage.txt
	rm -f lmd-*.html
	rm -rf vendor/
	rm -rf $(TOOLSFOLDER)

GOVET=$(GO) vet -all
SRCFOLDER=./cmd/. ./pkg/. ./buildtools/.
fmt: generate tools
	set -e; for CMD in $(CMDS); do \
		$(GOVET) ./cmd/$$CMD; \
	done
	set -e; for dir in $(shell ls -d1 pkg/*); do \
		$(GOVET) ./$$dir; \
	done
	gofmt -w -s $(SRCFOLDER)
	./tools/gofumpt -w $(SRCFOLDER)
	./tools/gci write --skip-generated $(SRCFOLDER)
	./tools/goimports -w $(SRCFOLDER)

generate: tools
	set -e; for dir in $(shell ls -d1 pkg/*); do \
		cd $$dir && go generate; \
	done

versioncheck:
	@[ $$( printf '%s\n' $(GOVERSION) $(MINGOVERSION) | sort | head -n 1 ) = $(MINGOVERSION) ] || { \
		echo "**** ERROR:"; \
		echo "**** $(PROJECT) requires at least golang version $(MINGOVERSIONSTR) or higher"; \
		echo "**** this is: $$(go version)"; \
		exit 1; \
	}

golangci: tools
	#
	# golangci combines a few static code analyzer
	# See https://github.com/golangci/golangci-lint
	#
	@set -e; for dir in $$(ls -1d pkg/* cmd); do \
		echo $$dir; \
		echo "  - GOOS=linux"; \
		( cd $$dir && GOOS=linux golangci-lint run --timeout=5m ./... ); \
	done

govulncheck: tools
	govulncheck ./...

version:
	OLDVERSION="$(shell grep "VERSION =" pkg/$(PROJECT)/main.go | awk '{print $$3}' | tr -d '"')"; \
	NEWVERSION=$$(dialog --stdout --inputbox "New Version:" 0 0 "v$$OLDVERSION") && \
		NEWVERSION=$$(echo $$NEWVERSION | sed "s/^v//g"); \
		if [ "v$$OLDVERSION" = "v$$NEWVERSION" -o "x$$NEWVERSION" = "x" ]; then echo "no changes"; exit 1; fi; \
		sed -i -e 's/VERSION =.*/VERSION = "'$$NEWVERSION'"/g' pkg/$(PROJECT)/main.go

zip: clean
	CGO_ENABLED=0 $(MAKE) build
	VERSION="$(shell grep "VERSION =" pkg/$(PROJECT)/main.go | awk '{print $$3}' | tr -d '"')"; \
		COMMITS="$(shell git rev-list $$(git describe --tags --abbrev=0)..HEAD --count)"; \
		DATE="$(shell LC_TIME=C date +%Y-%m-%d)"; \
		FILE="$$(printf "%s+git~%03d~%s_%s" $${VERSION} $${COMMITS} $(BUILD) $${DATE})"; \
		rm -f lmd-$$FILE.gz; \
		cp lmd lmd-$$FILE; \
		gzip -9 lmd-$$FILE; \
		ls -la lmd-$$FILE.gz; \
		echo "lmd-$$FILE.gz created";

# just skip unknown make targets
.DEFAULT:
	@if [[ "$(MAKECMDGOALS)" =~ ^testf ]]; then \
		: ; \
	else \
		echo "unknown make target(s): $(MAKECMDGOALS)"; \
		exit 1; \
	fi
