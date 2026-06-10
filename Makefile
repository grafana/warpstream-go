.PHONY: format
format:
	golangci-lint fmt

.PHONY: test
test:
	go test -race ./...

.PHONY: lint
lint:
	CGO_ENABLED=0 golangci-lint run
