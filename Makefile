#!/usr/bin/make -f

LAMPDDIR=lmd

all: build

deps: dump
	go get github.com/BurntSushi/toml
	go get github.com/kdar/factorlog
	go get github.com/mgutz/ansi
	go get golang.org/x/tools/cmd/goimports
	go get github.com/prometheus/client_golang/prometheus

dump:
	if [ $(shell grep -rc Dump $(LAMPDDIR)/*.go | grep -v :0 | grep -v $(LAMPDDIR)/dump.go | wc -l) -ne 0 ]; then \
		go get github.com/davecgh/go-spew/spew; \
		sed -i.bak 's/\/\/ +build.*/\/\/ build with debug functions/' $(LAMPDDIR)/dump.go; \
	else \
		sed -i.bak 's/\/\/ build.*/\/\/ +build ignore/' $(LAMPDDIR)/dump.go; \
	fi
	rm -f $(LAMPDDIR)/dump.go.bak

build: deps fmt
	cd $(LAMPDDIR) && go build -ldflags "-X main.Build=$(shell git rev-parse --short HEAD)"

debugbuild: deps fmt
	cd $(LAMPDDIR) && go build -race -ldflags "-X main.Build=$(shell git rev-parse --short HEAD)"

test: fmt dump
	cd $(LAMPDDIR) && go test -short -v | ../t/test_counter.sh
	if grep -r TODO: lmd/; then exit 1; fi

citest:
	cd $(LAMPDDIR) && go test -v | ../t/test_counter.sh
	cd $(LAMPDDIR) && go test -v -bench=B\* -run=^$$ . -benchmem
	if [ $$(cd $(LAMPDDIR) && gofmt -s -l . | wc -l) -gt 0 ]; then \
		echo "found format errors in these files:"; \
		cd $(LAMPDDIR) && gofmt -s -l .; \
		exit 1; \
	fi
	if grep -r TODO: lmd/; then exit 1; fi
	go get -u github.com/golang/lint/golint
	cd $(LAMPDDIR) && golint -set_exit_status .

benchmark: deps fmt
	cd $(LAMPDDIR) && go test -v -bench=B\* -run=^$$ . -benchmem

racetest: deps fmt
	cd $(LAMPDDIR) && go test -race -v $(COLORIZE_TEST)

covertest: deps fmt
	cd $(LAMPDDIR) && go test -v -coverprofile=cover.out $(COLORIZE_TEST)
	cd $(LAMPDDIR) && go tool cover -func=cover.out
	cd $(LAMPDDIR) && go tool cover -html=cover.out -o coverage.html

clean:
	rm -f $(LAMPDDIR)/lmd
	rm -f $(LAMPDDIR)/cover.out
	rm -f $(LAMPDDIR)/coverage.html

fmt:
	cd $(LAMPDDIR) && goimports -w .
	cd $(LAMPDDIR) && go vet
	cd $(LAMPDDIR) && gofmt -w -s .

lint:
	go get -u github.com/golang/lint/golint
	cd $(LAMPDDIR) && golint .

cyclo:
	go get github.com/fzipp/gocyclo
	cd $(LAMPDDIR) && gocyclo -over 15 .
