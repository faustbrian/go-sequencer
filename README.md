# sequencer

[![CI](https://github.com/faustbrian/go-sequencer/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/faustbrian/go-sequencer/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/CodeQL-required-blue)](https://github.com/faustbrian/go-sequencer/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Mutation](https://img.shields.io/badge/mutation-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Documentation](https://img.shields.io/badge/docs-checked_in_CI-blue)](docs/)
[![Go Reference](https://pkg.go.dev/badge/github.com/faustbrian/go-sequencer.svg)](https://pkg.go.dev/github.com/faustbrian/go-sequencer)
[![Release](https://img.shields.io/github/v/release/faustbrian/go-sequencer?sort=semver)](https://github.com/faustbrian/go-sequencer/releases)
[![Go](https://img.shields.io/badge/go-1.27.0-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`sequencer` is a durable orchestration library for one-time and explicitly
repeatable application operations. It keeps data changes separate from schema
migrations, compiles immutable dependency plans, and records every attempt
under fenced ownership.

The root module contains no global registry, reflection discovery, filesystem
scan, hidden worker, or implicit goroutine. Applications construct operations,
stores, runners, transport adapters, authentication, and dependencies.

The module follows stable v1 compatibility and requires Go 1.26.6 or later.

## Install

```sh
go get github.com/faustbrian/go-sequencer
```

## Five-minute quick start

The [standalone quick start](docs/quickstart.md) is a complete program using the
deterministic in-memory store. It compiles an immutable plan, runs one fenced
operation under a deadline, checks every error, and prints the execution result.

## Choose Sequencer or Scheduler

Use Sequencer for dependency-ordered, versioned, and checksummed application
operations that need a durable attempt ledger, fenced ownership, explicit
replay, and unknown-outcome reconciliation. It is intended for one-time and
explicitly repeated work, not recurring wall-clock decisions.

Use [Scheduler](https://github.com/faustbrian/go-scheduler) for recurring cron
or calendar decisions and coordinated dispatch across service replicas.
Applications that need both keep time selection in Scheduler and durable
operation execution in Sequencer; neither package hides the other behind a
global runtime.

## Packages

| Package | Use it for |
| --- | --- |
| `sequencer` | Immutable dependency plans, fenced durable attempts, synchronous runs, fleet execution, and reconciliation |
| `memory` | Deterministic process-local storage, leases, and queues for tests or non-durable work |
| `postgres` | Durable fenced state using a caller-owned PostgreSQL pool |
| `migrations` | Checking application-owned schema migration prerequisites |
| `scheduler` | Sending absolute future-eligibility requests to an application-owned scheduler |
| `adapters/queue` | Publishing or consuming identity-only operation requests with explicit settlement |
| `adapters/idempotency` | Protecting explicitly idempotent handlers through an application-owned durable gate |
| `adapters/lease` | Scoping singleton work to a caller-supplied fenced lease |
| `adapters/retry` | Running bounded inline retries against the shared attempt budget |
| `sequencehttp` | Authorized inspection, reset, and reconciliation HTTP controls |
| `sequencertest` | Deterministic clocks, operation fixtures, and fault-injecting test stores |

The released `goqueue`, `goidempotency`, `golease`, and `goretry` imports
remain compatibility facades for the longer of 180 days after successor
availability and two subsequently published stable minor releases.

PostgreSQL is the production reference store. The current integration paths
are explicit seams; applications still own database pools, queue settlement,
scheduler destinations, credentials, telemetry backends, and external side
effects.

## Lifecycle, ownership, and failures

`CompilePlan` validates and freezes definitions before work starts. `Runner`
executes synchronously. `Fleet` owns its bounded polling and lease-renewal
goroutines; cancel its `Run` context to begin draining and let the configured
shutdown wait bound graceful draining and the `Run` call. An uncooperative
drain-only handler may continue after that timeout until the process manager
terminates it. Constructors borrow stores and other injected collaborators, so
callers must keep them valid, synchronize mutable state, and close owned
resources after execution has stopped.

Handlers receive a context and a fenced attempt. Typed failures preserve their
in-process causes, while only redaction-safe classifications enter durable
records. Cancellation or lease expiry can make an external result
indeterminate; inspect and reconcile that exact attempt instead of assuming it
failed and replaying it. See the [lifecycle](docs/lifecycle.md),
[recovery](docs/recovery.md), and [security](docs/security.md) contracts before
production adoption.

Start with the [quickstart](docs/quickstart.md), then read the
[lifecycle](docs/lifecycle.md), [transaction](docs/transactions.md), and
[recovery](docs/recovery.md) contracts. Kubernetes deployments must also use
the [fleet operation contract](docs/kubernetes.md). All documentation is indexed in
[docs/README.md](docs/README.md).

The versioned [Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.6.2/docs/ecosystem/README.md)
and [Persistence and durability family](https://github.com/faustbrian/go-library-tools/blob/v1.6.2/docs/ecosystem/design-language.md#package-families-and-selection)
describe the shared design language, related packages, and composition rules.

The [API guide](docs/api.md), [cookbook](docs/cookbook.md),
[`sequencertest`](https://pkg.go.dev/github.com/faustbrian/go-sequencer/sequencertest),
[FAQ](docs/faq.md), [operations guide](docs/operations.md),
[compatibility notes](docs/compatibility.md), [changelog](CHANGELOG.md),
[support policy](SUPPORT.md), [security policy](SECURITY.md), and
[license](LICENSE) complete the adoption and support surface. Run `make check`
for the complete local gate.
