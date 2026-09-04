# sequencer

[![CI](https://github.com/faustbrian/go-sequencer/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/faustbrian/go-sequencer/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/CodeQL-required-blue)](https://github.com/faustbrian/go-sequencer/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Mutation](https://img.shields.io/badge/mutation-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Documentation](https://img.shields.io/badge/docs-checked_in_CI-blue)](docs/)
[![Go Reference](https://pkg.go.dev/badge/github.com/faustbrian/go-sequencer.svg)](https://pkg.go.dev/github.com/faustbrian/go-sequencer)
[![Release](https://img.shields.io/github/v/release/faustbrian/go-sequencer?sort=semver)](https://github.com/faustbrian/go-sequencer/releases)
[![Go](https://img.shields.io/badge/go-1.26.6-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`sequencer` is a durable orchestration library for one-time and explicitly
repeatable application operations. It keeps data changes separate from schema
migrations, compiles immutable dependency plans, and records every attempt
under fenced ownership.

The root module contains no global registry, reflection discovery, filesystem
scan, hidden worker, or implicit goroutine. Applications construct operations,
stores, runners, transport adapters, authentication, and dependencies.

```go
operation := sequencer.OperationSpec{
    ID: "postal.normalize-postcodes", Version: 1,
    Checksum: "sha256:reviewed-source-checksum",
    Description: "Normalize stored postcode spelling", Channel: "deploy",
    Policy: sequencer.Policy{
        Mode: sequencer.OneTime, MaxAttempts: 3, MaxExceptions: 3,
        Timeout: time.Minute,
    },
    Handler: sequencer.HandlerFunc(func(ctx context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
        return sequencer.Output{Summary: "normalized postcodes"}, nil
    }),
}
plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{operation}, sequencer.PlanOptions{})
if err != nil { /* fail deployment */ }
runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: replicaID})
if err != nil { /* fail deployment */ }
report, err := runner.Execute(ctx)
```

PostgreSQL is the production reference store. `memory` is a deterministic
reference adapter. `goqueue`, `scheduler`, `goretry`, `golease`, and
`goidempotency` are explicit integration seams. `migrations` asserts schema
prerequisites without owning migration history. `sequencehttp` requires an
application authorizer for every administrative action.

Start with the [quickstart](docs/quickstart.md), then read the
[lifecycle](docs/lifecycle.md), [transaction](docs/transactions.md), and
[recovery](docs/recovery.md) contracts. Kubernetes deployments must also use
the [fleet operation contract](docs/kubernetes.md). All documentation is indexed in
[docs/README.md](docs/README.md).

The versioned [Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and [Persistence and durability family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection)
describe the shared design language, related packages, and composition rules.

Requires Go 1.26.6. Run `make check` for the complete local gate.
