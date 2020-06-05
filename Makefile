#!/usr/bin/make -f

LAMPDDIR=lmd
MAKE:=make
SHELL:=bash
GOVERSION:=$(shell \
    go version | \
    awk -F'go| ' '{ split($$5, a, /\./); printf ("%04d%04d", a[1], a[2]); exit; }' \
)
MINGOVERSION:=00010014
MINGOVERSIONSTR:=1.14

all: build

tools: versioncheck vendor dump
	go mod download
	set -e; for DEP in $(shell grep _ buildtools/tools.go | awk '{ print $$2 }'); do \
		go get $$DEP; \
	done
	go mod vendor
	go mod tidy

updatedeps: versioncheck
	$(MAKE) clean
	go list -u -m all
	go mod download
	set -e; for DEP in $(shell grep _ buildtools/tools.go | awk '{ print $$2 }'); do \
		go get -u $$DEP; \
	done
	go mod tidy

vendor:
	go mod download
	go mod vendor
	go mod tidy

dump:
	if [ $(shell grep -rc Dump $(LAMPDDIR)/*.go | grep -v :0 | grep -v $(LAMPDDIR)/dump.go | wc -l) -ne 0 ]; then \
		sed -i.bak 's/\/\/ +build.*/\/\/ build with debug functions/' $(LAMPDDIR)/dump.go; \
	else \
		sed -i.bak 's/\/\/ build.*/\/\/ +build ignore/' $(LAMPDDIR)/dump.go; \
	fi
	rm -f $(LAMPDDIR)/dump.go.bak

build: vendor
	cd $(LAMPDDIR) && go build -ldflags "-s -w -X main.Build=$(shell git rev-parse --short HEAD)"

build-linux-amd64: vendor
	cd $(LAMPDDIR) && GOOS=linux GOARCH=amd64 go build -ldflags "-s -w -X main.Build=$(shell git rev-parse --short HEAD)" -o lmd.linux.amd64

debugbuild: fmt dump vendor
	cd $(LAMPDDIR) && go build -race -ldflags "-X main.Build=$(shell git rev-parse --short HEAD)"

devbuild: debugbuild

test: fmt dump vendor
	cd $(LAMPDDIR) && go test -short -v | ../t/test_counter.sh
	rm -f lmd/mock*.sock
	if grep -rn TODO: lmd/; then exit 1; fi
	if grep -rn Dump lmd/*.go | grep -v dump.go; then exit 1; fi

longtest: fmt dump vendor
	cd $(LAMPDDIR) && go test -v | ../t/test_counter.sh
	rm -f lmd/mock*.sock

citest: vendor
	rm -f lmd/mock*.sock
	#
	# Checking gofmt errors
	#
	if [ $$(cd $(LAMPDDIR) && gofmt -s -l . | wc -l) -gt 0 ]; then \
		echo "found format errors in these files:"; \
		cd $(LAMPDDIR) && gofmt -s -l .; \
		exit 1; \
	fi
	#
	# Checking TODO items
	#
	if grep -rn TODO: lmd/; then exit 1; fi
	#
	# Checking remaining debug calls
	#
	if grep -rn Dump lmd/*.go | grep -v dump.go; then exit 1; fi
	#
	# Run other subtests
	#
	$(MAKE) copyfighter
	$(MAKE) golangci
	$(MAKE) fmt
	#
	# Normal test cases
	#
	cd $(LAMPDDIR) && go test -v | ../t/test_counter.sh
	#
	# Benchmark tests
	#
	cd $(LAMPDDIR) && go test -v -bench=B\* -run=^$$ . -benchmem
	#
	# Race rondition tests
	#
	$(MAKE) racetest
	#
	# All CI tests successfull
	#
	go mod tidy

benchmark: fmt
	cd $(LAMPDDIR) && go test -ldflags "-s -w -X main.Build=$(shell git rev-parse --short HEAD)" -v -bench=B\* -benchtime 10s -run=^$$ . -benchmem

racetest: fmt
	cd $(LAMPDDIR) && go test -race -short -v

covertest: fmt
	cd $(LAMPDDIR) && go test -v -coverprofile=cover.out
	cd $(LAMPDDIR) && go tool cover -func=cover.out
	cd $(LAMPDDIR) && go tool cover -html=cover.out -o coverage.html

coverweb: fmt
	cd $(LAMPDDIR) && go test -v -coverprofile=cover.out
	cd $(LAMPDDIR) && go tool cover -html=cover.out

clean:
	rm -f $(LAMPDDIR)/lmd
	rm -f $(LAMPDDIR)/cover.out
	rm -f $(LAMPDDIR)/coverage.html
	rm -f $(LAMPDDIR)/*.sock
	rm -f lmd-*.html
	rm -rf vendor

fmt: generate tools
	cd $(LAMPDDIR) && goimports -w .
	cd $(LAMPDDIR) && go vet -all -assign -atomic -bool -composites -copylocks -nilfunc -rangeloops -unsafeptr -unreachable .
	cd $(LAMPDDIR) && gofmt -w -s .

generate: tools
	cd $(LAMPDDIR) && go generate

versioncheck:
	@[ $$( printf '%s\n' $(GOVERSION) $(MINGOVERSION) | sort | head -n 1 ) = $(MINGOVERSION) ] || { \
		echo "**** ERROR:"; \
		echo "**** LMD requires at least golang version $(MINGOVERSIONSTR) or higher"; \
		echo "**** this is: $$(go version)"; \
		exit 1; \
	}

copyfighter: tools
	#
	# Check if there are values better passed as pointer
	# See https://github.com/jmhodges/copyfighter
	#
	#cd $(LAMPDDIR) && copyfighter .

golangci: tools
	#
	# golangci combines a few static code analyzer
	# See https://github.com/golangci/golangci-lint
	#
	golangci-lint run $(LAMPDDIR)/...

version:
	OLDVERSION="$(shell grep "VERSION =" $(LAMPDDIR)/main.go | awk '{print $$3}' | tr -d '"')"; \
	NEWVERSION=$$(dialog --stdout --inputbox "New Version:" 0 0 "v$$OLDVERSION") && \
		NEWVERSION=$$(echo $$NEWVERSION | sed "s/^v//g"); \
		if [ "v$$OLDVERSION" = "v$$NEWVERSION" -o "x$$NEWVERSION" = "x" ]; then echo "no changes"; exit 1; fi; \
		sed -i -e 's/VERSION =.*/VERSION = "'$$NEWVERSION'"/g' $(LAMPDDIR)/main.go
