#!/usr/bin/make -f

LAMPDDIR=lmd
MAKE:=make
SHELL:=bash
GOVERSION:=$(shell \
    go version | \
    awk -F'go| ' '{ split($$5, a, /\./); printf ("%04d%04d", a[1], a[2]); exit; }' \
)
MINGOVERSION:=00010010
MINGOVERSIONSTR:=1.10

EXTERNAL_DEPS = \
	github.com/BurntSushi/toml \
	github.com/kdar/factorlog \
	github.com/mgutz/ansi \
	github.com/prometheus/client_golang/prometheus \
	github.com/prometheus/client_golang/prometheus/promhttp \
	github.com/buger/jsonparser \
	github.com/a8m/djson \
	github.com/julienschmidt/httprouter \
	github.com/davecgh/go-spew/spew \
	golang.org/x/tools/cmd/goimports \
	golang.org/x/lint/golint \
	github.com/fzipp/gocyclo \
	github.com/client9/misspell/cmd/misspell \
	github.com/jmhodges/copyfighter \
	github.com/mvdan/unparam \
	github.com/mdempsky/unconvert \
	honnef.co/go/tools/cmd/staticcheck \


all: deps fmt build

deps: versioncheck dump
	set -e; for DEP in $(EXTERNAL_DEPS); do \
		go get $$DEP; \
	done

updatedeps: versioncheck
	set -e; for DEP in $(EXTERNAL_DEPS); do \
		go get -u $$DEP; \
	done

dump:
	if [ $(shell grep -rc Dump $(LAMPDDIR)/*.go | grep -v :0 | grep -v $(LAMPDDIR)/dump.go | wc -l) -ne 0 ]; then \
		sed -i.bak 's/\/\/ +build.*/\/\/ build with debug functions/' $(LAMPDDIR)/dump.go; \
	else \
		sed -i.bak 's/\/\/ build.*/\/\/ +build ignore/' $(LAMPDDIR)/dump.go; \
	fi
	rm -f $(LAMPDDIR)/dump.go.bak

build: dump
	cd $(LAMPDDIR) && go build -ldflags "-s -w -X main.Build=$(shell git rev-parse --short HEAD)"

build-linux-amd64: dump
	cd $(LAMPDDIR) && GOOS=linux GOARCH=amd64 go build -ldflags "-s -w -X main.Build=$(shell git rev-parse --short HEAD)" -o lmd.linux.amd64

debugbuild: deps fmt
	cd $(LAMPDDIR) && go build -race -ldflags "-X main.Build=$(shell git rev-parse --short HEAD)"

test: fmt dump
	cd $(LAMPDDIR) && go test -short -v | ../t/test_counter.sh
	rm -f lmd/mock*.sock
	if grep -rn TODO: lmd/; then exit 1; fi
	if grep -rn Dump lmd/*.go | grep -v dump.go; then exit 1; fi

longtest: fmt dump
	cd $(LAMPDDIR) && go test -v | ../t/test_counter.sh
	rm -f lmd/mock*.sock

citest: deps
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
	$(MAKE) lint
	$(MAKE) cyclo
	$(MAKE) misspell
	$(MAKE) copyfighter
	$(MAKE) unparam
	$(MAKE) unconvert
	$(MAKE) staticcheck
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

benchmark: fmt
	cd $(LAMPDDIR) && go test -ldflags "-s -w -X main.Build=$(shell git rev-parse --short HEAD)" -v -bench=B\* -run=^$$ . -benchmem

racetest: fmt
	cd $(LAMPDDIR) && go test -race -v

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

fmt:
	cd $(LAMPDDIR) && goimports -w .
	cd $(LAMPDDIR) && go vet -all -assign -atomic -bool -composites -copylocks -nilfunc -rangeloops -unsafeptr -unreachable .
	cd $(LAMPDDIR) && gofmt -w -s .

versioncheck:
	@[ $$( printf '%s\n' $(GOVERSION) $(MINGOVERSION) | sort | head -n 1 ) = $(MINGOVERSION) ] || { \
		echo "**** ERROR:"; \
		echo "**** LMD requires at least golang version $(MINGOVERSIONSTR) or higher"; \
		echo "**** this is: $$(go version)"; \
		exit 1; \
	}

lint:
	#
	# Check if golint complains
	# see https://github.com/golang/lint/ for details.
	cd $(LAMPDDIR) && golint -set_exit_status .

cyclo:
	#
	# Check if there are any too complicated functions
	# Any function with a score higher than 20 is considered bad.
	# See https://github.com/fzipp/gocyclo for details.
	#
	cd $(LAMPDDIR) && gocyclo -over 20 . | ../t/filter_cyclo_exceptions.sh

misspell:
	#
	# Check if there are common spell errors.
	# See https://github.com/client9/misspell
	#
	cd $(LAMPDDIR) && misspell -error .

copyfighter:
	#
	# Check if there are values better passed as pointer
	# See https://github.com/jmhodges/copyfighter
	#
	cd $(LAMPDDIR) && copyfighter .

unparam:
	#
	# Check if all function parameters are actually used
	# See https://github.com/mvdan/unparam
	#
	@if [ $$( printf '%s\n' $(GOVERSION) 00010010 | sort -n | head -n 1 ) != 00010010 ]; then \
		echo "unparam requires at least go 1.10"; \
	else \
		cd $(LAMPDDIR) && unparam -exported . ; \
	fi

unconvert:
	#
	# The unconvert program analyzes Go packages to identify unnecessary type conversions
	# See https://github.com/mdempsky/unconvert
	#
	cd $(LAMPDDIR) && unconvert -v

staticcheck:
	#
	# staticcheck combines a few static code analyzer
	# See honnef.co/go/tools/cmd/staticcheck
	#
	@if [ $$( printf '%s\n' $(GOVERSION) 00010010 | sort -n | head -n 1 ) != 00010010 ]; then \
		echo "staticcheck requires at least go 1.10"; \
	else \
		cd $(LAMPDDIR) && staticcheck . ; \
	fi

goreporter: clean
	#
	# The goreporter program creates a static-analyisis report
	# See https://github.com/360EntSecGroup-Skylar/goreporter
	#
	go get -u github.com/360EntSecGroup-Skylar/goreporter
	cd $(LAMPDDIR) && goreporter -p . -r ../

version:
	OLDVERSION="$(shell grep "VERSION =" $(LAMPDDIR)/main.go | awk '{print $$3}' | tr -d '"')"; \
	NEWVERSION=$$(dialog --stdout --inputbox "New Version:" 0 0 "v$$OLDVERSION") && \
		NEWVERSION=$$(echo $$NEWVERSION | sed "s/^v//g"); \
		if [ "v$$OLDVERSION" = "v$$NEWVERSION" -o "x$$NEWVERSION" = "x" ]; then echo "no changes"; exit 1; fi; \
		sed -i -e 's/VERSION =.*/VERSION = "'$$NEWVERSION'"/g' $(LAMPDDIR)/main.go
