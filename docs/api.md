# API

`OperationSpec` contains stable identity, version, checksum, description, tags,
channel, dependencies, environments, execution policy, an optional condition,
and a handler. `NewOperation` validates and freezes its slices.

`CompilePlan` rejects invalid definitions, duplicates, missing dependencies,
cycles, and resource-limit violations. `Plan.IDs`, `Plan.Operations`, and
`Plan.Operation` return defensive copies in deterministic order.

`Store` is the root durability contract. Registration fails on checksum drift.
Claims include owner and fencing proof. Every mutation after claim requires
that proof. Attempts and audit events are bounded inspection surfaces.

`Runner.Execute` registers a complete plan and runs it synchronously. Reports
retain every terminal operation, including allowed failures. Handlers receive
an `Attempt`; a transaction appears only when the operation explicitly enables
`WithinTransaction` and a transaction manager is injected.

`Fleet` owns a long-running, leaderless claim loop. `State` exposes starting,
accepting, draining, stopped, and failed; `Ready` is true only while accepting.
`FleetOptions` bounds claim polling, per-pod concurrency, lease renewal, and
shutdown wait. `LeaseStore.RenewLease` extends only claimed or running ownership
under the current owner and fencing token.

`Policy.RetryMode` chooses the only retry loop. Inline mode supplies a
concurrency-safe `ExecutionBudget` through `Attempt.Budget`; durable mode keeps
retry attempts visible in the ledger. `CancellationDrainOnly` withholds
shutdown cancellation for unsafe operations and makes timeout recovery
explicitly unknown.

Typed constructors classify permanent, retryable, skipped, blocked,
unknown-result, and rollback failures while preserving the in-process cause.
Only redaction-safe classifications are persisted by the runner.
