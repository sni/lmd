

all: build

LAMPDDIR=lampd

deps:
	go get github.com/BurntSushi/toml
	go get github.com/kdar/factorlog
	go get github.com/mgutz/ansi
	go get golang.org/x/tools/cmd/goimports

build: deps fmt
	cd $(LAMPDDIR) && go build -ldflags "-X main.Build=$(shell git rev-parse --short HEAD)"

test: deps fmt
	cd $(LAMPDDIR) && go test -v

clean:
	rm -f $(LAMPDDIR)/lampd

fmt:
	cd $(LAMPDDIR) && goimports -w .
	cd $(LAMPDDIR) && go fmt
