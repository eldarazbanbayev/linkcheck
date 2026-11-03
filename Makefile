.PHONY: build vet test

build:
	go build -o bin/linkcheck ./cmd/linkcheck

vet:
	go vet ./...

test:
	go test -race -count=1 ./...
