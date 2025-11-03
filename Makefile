.PHONY: build test lint clean run

build:
	@go build -o bin/go-pq-cdc-pq .

test:
	@go test -v ./...

lint:
	@golangci-lint run

clean:
	@rm -rf bin/
	@go clean

run:
	@go run .

