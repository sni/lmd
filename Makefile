all: build

LAMPDDIR=lampd

INTERACTIVE:=$(shell [ -t 0 ] && echo 1)
ifdef INTERACTIVE
COLORIZE_TEST=| sed ''/PASS/s//$$(printf "\033[32mPASS\033[0m")/'' | sed ''/FAIL/s//$$(printf "\033[31mFAIL\033[0m")/''
else
COLORIZE_TEST=
endif

deps:
	go get github.com/BurntSushi/toml
	go get github.com/kdar/factorlog
	go get github.com/mgutz/ansi
	go get golang.org/x/tools/cmd/goimports
	if [ $(shell grep -rc Dump lampd/*.go | grep -v :0 | grep -v lampd/dump.go | wc -l) -ne 0 ]; then \
		go get github.com/davecgh/go-spew/spew; \
		sed -i lampd/dump.go -e "s/\/\/ +build.*/\/\/ build with debug functions/"; \
	else \
		sed -i lampd/dump.go -e "s/\/\/ build.*/\/\/ +build ignore/"; \
	fi

build: deps fmt
	cd $(LAMPDDIR) && go build -ldflags "-X main.Build=$(shell git rev-parse --short HEAD)"

test: deps fmt
	cd $(LAMPDDIR) && go test -v $(COLORIZE_TEST)

clean:
	rm -f $(LAMPDDIR)/lampd

fmt:
	cd $(LAMPDDIR) && goimports -w .
	cd $(LAMPDDIR) && go fmt
