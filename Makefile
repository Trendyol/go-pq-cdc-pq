.PHONY: build test lint clean run podman-up podman-down podman-logs podman-ps podman-stop podman-start podman-restart podman-clean podman-target-test

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
