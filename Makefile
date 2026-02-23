.PHONY: build test test-integration lint clean run podman-up podman-down podman-logs podman-ps podman-stop podman-start podman-restart podman-clean podman-target-test

build:
	@go build -o bin/go-pq-cdc-pq .

test:
	@GOSUMDB=sum.golang.org GOPROXY=https://proxy.golang.org,direct go test -v ./...

test-integration:
	@GOSUMDB=sum.golang.org GOPROXY=https://proxy.golang.org,direct go test -v -tags=integration -timeout 5m ./...

lint:
	@golangci-lint run

clean:
	@rm -rf bin/
	@go clean

run:
	@go run .
