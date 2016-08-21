

all: build

LAMPDDIR=lampd

deps:
	for dep in $$(cd $(LAMPDDIR) && go list -f '{{join .Deps "\n"}}' | xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}'); do \
		echo $$dep; \
		go get $$dep; \
	done

build: deps fmt
	cd $(LAMPDDIR) && go build -ldflags "-X main.Build=$(shell git rev-parse --short HEAD)"

clean:
	rm -f $(LAMPDDIR)/lampd

fmt:
	cd $(LAMPDDIR) && go fmt
