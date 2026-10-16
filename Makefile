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
	mkdir -p pkg/internal/simulation/.reports
	go run ./pkg/internal/simulation -report-filepath pkg/internal/simulation/.reports/report.md -json-report-filepath pkg/internal/simulation/.reports/report.json
