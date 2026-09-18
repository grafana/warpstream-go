.PHONY: format
format:
	golangci-lint fmt

.PHONY: test
test:
	go test -race ./...

.PHONY: lint
lint:
	CGO_ENABLED=0 golangci-lint run

.PHONY: simulation
simulation:
	go run ./pkg/internal/simulation -report-filepath pkg/internal/simulation/REPORT.md
