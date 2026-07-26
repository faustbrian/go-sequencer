# Goal: Durable Ordered Operation Sequencing

## Objective

Build `sequencer` as a durable, production-grade orchestration package for
one-time and explicitly repeatable operations that must execute in a declared
order across deployments. It replaces Laravel one-time operations and Cline
Sequencer without conflating data operations with database schema migrations.

## Core Model

- Stable operation identifier, version, checksum, description, tags, channel,
  dependencies, environment constraints, and execution policy.
- Explicit operation states: pending, eligible, claimed, running, succeeded,
  skipped, failed, retryable, deferred, canceled, rolled back, and blocked.
- Durable append-only attempts and current-state projection.
- Dependency graph validation, deterministic topological ordering, cycle
  detection, missing dependency handling, and stable tie-breaking.
- Operation handler receiving context, attempt metadata, logger/observer seams,
  and caller-provided dependencies without a service locator.
- Immutable execution plan that can be inspected before work begins.

## Execution Modes

- Synchronous in-process execution with bounded cancellation and cleanup.
- Durable asynchronous execution through an optional `queue` adapter.
- Deferred and scheduled eligibility through explicit clocks and optional
  `scheduler` integration.
- Per-operation retries through `retry`, with typed retryability and budgets.
- Singleton/distributed claims through `lease` with fencing where supported.
- Idempotent operation integration through `idempotency` where required.
- Manual approval, conditional execution, allowed failure, and skip policies
  only when declared and auditable.

An asynchronous operation cannot share a live database transaction with the
process that enqueued it or with later operations. `WithinTransaction` means
one worker attempt executes inside one local transaction. Cross-operation
atomicity MUST NOT be claimed.

## Persistence And Transactions

- Root storage interfaces with PostgreSQL as the production reference adapter.
- Versioned ledger schema, checksums, timestamps, ownership proofs, attempt
  numbers, errors, outputs, and audit metadata.
- Claim-next and transition operations must be transactional and concurrency
  safe.
- Operation code MAY receive an injected transaction/session through an
  explicit adapter; the sequencer does not discover repositories globally.
- Schema migrations remain in `migrations`; sequenced operations MAY be
  placed between schema changes by deployment orchestration.
- Provide a migration bridge that can assert prerequisite schema versions
  without owning migration history.

## Failure And Recovery

- Typed permanent, retryable, skip, blocked, stale-owner, canceled, timeout,
  unknown-result, and rollback errors.
- Crash recovery based on durable lease/attempt state, never process memory.
- Explicit max attempts, max exceptions, timeout, backoff, and dead-letter
  behavior.
- Optional rollback handlers are compensations, not database time travel.
- Re-running a succeeded one-time operation requires an explicit audited reset
  or new version; checksum drift fails closed.
- Partial batches and allowed failures remain visible in final reports.

## Package Shape

- Root: operations, plans, states, transitions, errors, runner contracts.
- `postgres`: durable ledger and transactional claims.
- `goqueue`: optional asynchronous execution adapter.
- `scheduler`: optional deferred/scheduled adapter.
- `migrations`: explicit deployment bridge, not a migration engine.
- `sequencehttp` or CLI package only for inspect/status/execute controls with
  authorization supplied by applications.
- `sequencertest`: deterministic operations, clocks, stores, and fault helpers.

## Security And Bounds

- Bound operations, dependencies, graph depth, attempts, output, error detail,
  tags, concurrency, waiters, history, and retention.
- Do not persist secrets, payloads, stack traces, or arbitrary errors by
  default.
- Administrative execution and reset actions require application-owned
  authentication and authorization.
- No reflection discovery, filesystem scanning, global registry, hidden worker,
  implicit goroutine, or package-level mutable state.

## Verification And Documentation

Meaningful 100% production coverage is mandatory. Add state-machine and graph
property tests, PostgreSQL conformance, concurrent claimant tests, crash and
unknown-result injection, queue redelivery, transaction rollback, lease expiry,
checksum drift, retry/dead-letter, cancellation, race, fuzz, mutation, leak,
and capacity benchmarks.

Document API, operation lifecycle, ordering, transactions, async execution,
deployment sequencing, migration integration, retries, rollback, recovery,
operations, security, performance, Laravel migration, cookbook, FAQ,
compatibility, and changelog. Local and CI gates follow ecosystem standards.

## Acceptance Criteria

- Postal and Location can replace one-time operations without replaying or
  losing execution history.
- Concurrent replicas execute each claimed attempt under explicit ownership.
- Dependencies and ordering are deterministic and crash-safe.
- Synchronous transaction and asynchronous durability boundaries are honest.
- Meaningful 100% coverage and every blocking gate pass.
