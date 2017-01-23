#!/usr/bin/make -f

LAMPDDIR=lmd
MAKE:=make
SHELL:=bash
GOVERSION:=$(shell go version | awk '{print $$3}' | sed 's/^go\([0-9]\.[0-9]\).*/\1/')

all: deps fmt build

deps: versioncheck dump
	go get github.com/BurntSushi/toml
	go get github.com/kdar/factorlog
	go get github.com/mgutz/ansi
	go get golang.org/x/tools/cmd/goimports
	go get github.com/prometheus/client_golang/prometheus
	go get github.com/Jeffail/gabs

updatedeps: versioncheck
	go get -u github.com/BurntSushi/toml
	go get -u github.com/kdar/factorlog
	go get -u github.com/mgutz/ansi
	go get -u golang.org/x/tools/cmd/goimports
	go get -u github.com/prometheus/client_golang/prometheus
	go get -u github.com/Jeffail/gabs
	go get -u github.com/davecgh/go-spew/spew



dump:
	if [ $(shell grep -rc Dump $(LAMPDDIR)/*.go | grep -v :0 | grep -v $(LAMPDDIR)/dump.go | wc -l) -ne 0 ]; then \
		go get github.com/davecgh/go-spew/spew; \
		sed -i.bak 's/\/\/ +build.*/\/\/ build with debug functions/' $(LAMPDDIR)/dump.go; \
	else \
		sed -i.bak 's/\/\/ build.*/\/\/ +build ignore/' $(LAMPDDIR)/dump.go; \
	fi
	rm -f $(LAMPDDIR)/dump.go.bak

build: dump
	cd $(LAMPDDIR) && go build -ldflags "-s -w -X main.Build=$(shell git rev-parse --short HEAD)"

debugbuild: deps fmt
	cd $(LAMPDDIR) && go build -race -ldflags "-X main.Build=$(shell git rev-parse --short HEAD)"

test: fmt dump
	cd $(LAMPDDIR) && go test -short -v | ../t/test_counter.sh
	if grep -r TODO: lmd/; then exit 1; fi

citest: deps
	#
	# Normal test cases
	#
	cd $(LAMPDDIR) && go test -v | ../t/test_counter.sh
	#
	# Benchmark tests
	#
	cd $(LAMPDDIR) && go test -v -bench=B\* -run=^$$ . -benchmem
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
	if grep -r TODO: lmd/; then exit 1; fi
	$(MAKE) lint
	$(MAKE) cyclo
	$(MAKE) mispell
	#
	# All CI tests successfull
	#

benchmark: fmt
	cd $(LAMPDDIR) && go test -v -bench=B\* -run=^$$ . -benchmem

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

fmt:
	cd $(LAMPDDIR) && goimports -w .
	cd $(LAMPDDIR) && go tool vet -all -shadow -assign -atomic -bool -composites -copylocks -nilfunc -rangeloops -unsafeptr -unreachable .
	cd $(LAMPDDIR) && gofmt -w -s .

versioncheck:
	@[ "$(GOVERSION)" = "devel" ] || [ $$(echo "$(GOVERSION)" | tr -d ".") -ge 15 ] || { \
		echo "**** ERROR:"; \
		echo "**** LMD requires at least golang version 1.5 or higher"; \
		echo "**** this is: $$(go version)"; \
		exit 1; \
	}

lint:
	#
	# Check if golint complains
	# see https://github.com/golang/lint/ for details.
	# Only works with Go 1.6 or up.
	#
	@( [ "$(GOVERSION)" != "devel" ] && [ $$(echo "$(GOVERSION)" | tr -d ".") -lt 16 ] ) || { \
		go get -u github.com/golang/lint/golint; \
		cd $(LAMPDDIR) && golint -set_exit_status .; \
	}

cyclo:
	go get -u github.com/fzipp/gocyclo
	#
	# Check if there are any too complicated functions
	# Any function with a score higher than 15 is bad.
	# See https://github.com/fzipp/gocyclo for details.
	#
	cd $(LAMPDDIR) && gocyclo -over 15 . | ../t/filter_cyclo_exceptions.sh

mispell:
	go get -u github.com/client9/misspell/cmd/misspell
	#
	# Check if there are common spell errors.
	# See https://github.com/client9/misspell
	#
	cd $(LAMPDDIR) && misspell -error .

version:
	OLDVERSION="$(shell grep "VERSION =" $(LAMPDDIR)/main.go | awk '{print $$3}' | tr -d '"')"; \
	NEWVERSION=$$(dialog --stdout --inputbox "New Version:" 0 0 "v$$OLDVERSION") && \
		NEWVERSION=$$(echo $$NEWVERSION | sed "s/^v//g"); \
		if [ "v$$OLDVERSION" = "v$$NEWVERSION" -o "x$$NEWVERSION" = "x" ]; then echo "no changes"; exit 1; fi; \
		sed -i -e 's/VERSION =.*/VERSION = "'$$NEWVERSION'"/g' $(LAMPDDIR)/main.go
