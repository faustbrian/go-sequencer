GO ?= go
FUZZ_TIME ?= 2s
BENCH_TIME ?= 100ms
GOLANGCI_LINT_VERSION ?= v2.12.2
STATICCHECK_VERSION ?= v0.8.0-rc.1
GOVULNCHECK_VERSION ?= v1.6.0
GREMLINS_VERSION ?= v0.6.0
ACTIONLINT_VERSION ?= v1.7.12

.PHONY: benchmark check coverage docs format format-check fuzz integration \
	lint mutation race safety staticcheck test tidy-check vet vuln workflows

format:
	gofmt -w .

format-check:
	test -z "$$(gofmt -l .)"

tidy-check:
	GOWORK=off $(GO) mod tidy -diff

test:
	GOWORK=off $(GO) test ./...

integration:
	GOWORK=off $(GO) test -tags=integration -timeout=10m ./...

race:
	GOWORK=off $(GO) test -race ./...
	GOWORK=off $(GO) test -race -tags=integration -timeout=10m ./postgres

coverage:
	./scripts/check-coverage.sh

fuzz:
	./scripts/check-fuzz.sh "$(FUZZ_TIME)"

mutation:
	./scripts/check-mutation.sh

benchmark:
	GOWORK=off $(GO) test ./... -run '^$$' -bench . -benchmem -benchtime="$(BENCH_TIME)"

vet:
	GOWORK=off $(GO) vet ./...

staticcheck:
	GOWORK=off $(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...

lint:
	GOWORK=off $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run --timeout=5m

vuln:
	GOWORK=off $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

docs:
	./scripts/check-docs.sh

safety:
	./scripts/check-safety.sh

workflows:
	GOWORK=off $(GO) run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION) .github/workflows/*.yml

check: tidy-check format-check vet staticcheck lint test integration race \
	coverage fuzz mutation benchmark docs safety vuln workflows
