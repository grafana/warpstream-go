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

.PHONY: simulation-mimir-build
simulation-mimir-build:
	bash pkg/internal/simulation/mimir/build-mimir.sh

.PHONY: simulation-mimir
simulation-mimir: simulation-mimir-build
	mkdir -p pkg/internal/simulation/mimir/.reports
	cd pkg/internal/simulation/mimir && go run -mod=mod . -mimir-binary "$(CURDIR)/pkg/internal/simulation/mimir/.build/mimir" -timeout 20m -report-filepath .reports/report.md -json-report-filepath .reports/report.json

.PHONY: simulation-mimir-investigation
simulation-mimir-investigation: simulation-mimir-build
	mkdir -p pkg/internal/simulation/mimir/.reports
	cd pkg/internal/simulation/mimir && go run -mod=mod . -mimir-binary "$(CURDIR)/pkg/internal/simulation/mimir/.build/mimir" -profile mimir-investigation -parallel 1 -check=false -timeout 20m -report-filepath .reports/investigation.md -json-report-filepath .reports/investigation.json
