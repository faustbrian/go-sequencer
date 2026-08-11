# API

`OperationSpec` contains stable identity, version, checksum, description, tags,
channel, exact dependency references, environments, execution policy, an
optional exact `Compensates` reference, a condition, and a handler.
`NewOperation` validates and defensively freezes those definitions. A
compensation reference must also be one of the operation's dependencies.
`OperationID.Valid` exposes the same lowercase 255-byte identifier grammar to
stores and transport adapters. Checksums are limited to 512 bytes,
descriptions to 4 KiB, and individual tags and environment selectors to 255
bytes; tag and environment collections are each limited to 64 entries.

`CompilePlan` rejects invalid definitions, duplicates, missing dependencies,
cycles, and resource-limit violations. `Plan.IDs`, `Plan.Operations`, and
`Plan.Operation` return defensive copies in deterministic order.

`Store` is the root durability contract. Registration fails on checksum drift.
Claims include owner and fencing proof. Every mutation after claim requires
that proof. `Completion.From` selects the fenced source state and defaults to
`Running`; runners use `Claimed` only to settle an exhausted claim without
claiming handler execution. `Claim.Budget` contains the durable attempt and
prior retry-exception counts for the current replay epoch; `Record` exposes the
persisted counters.
Attempts and audit events are bounded inspection surfaces.
`ReconciliationStore` optionally adds `ResolveUnknown`; each request is bounded,
attributed, and bound to the exact operation version, attempt, and fencing token.

`Runner.Execute` registers a complete plan and runs it synchronously. Reports
retain every terminal operation, including allowed failures. Handlers receive
an `Attempt`; a transaction appears only when the operation explicitly enables
`WithinTransaction` and a transaction manager is injected. Re-executing a
successful `Repeatable` operation writes an attributed reset before claiming;
`OneTime` success remains complete.

`Fleet` owns a long-running, leaderless claim loop. `State` exposes starting,
accepting, draining, stopped, and failed; `Ready` is true only while accepting.
`FleetOptions` bounds claim polling, per-pod concurrency, lease renewal, and
shutdown wait. `LeaseStore.RenewLease` extends only claimed or running ownership
under the current owner and fencing token. Fleet store calls have per-call
deadlines even though the owning `Run` context is intentionally long-lived.

`Policy.RetryMode` chooses the only retry loop. Inline mode supplies a
concurrency-safe `ExecutionBudget` through `Attempt.Budget`; durable mode keeps
retry attempts visible in the ledger and marks typed retry completions so the
store advances the independent exception count. `CancellationDrainOnly` withholds
shutdown cancellation for unsafe operations and makes timeout recovery
explicitly unknown.

`Policy.UnknownOutcome` defaults to blocking expired unknown work as
`Indeterminate`. `UnknownOutcomeReplayIdempotent` explicitly authorizes
automatic eligibility after the indeterminate audit boundary. `DeadLetter`
makes terminal failures observable as `DeadLettered`. Canceled and dead-lettered
records can be replayed only through an attributed reset.

Typed constructors classify permanent, retryable, skipped, blocked,
unknown-result, and rollback failures while preserving the in-process cause.
Only redaction-safe classifications are persisted by the runner.

`sequencehttp` requires an application authorizer to return the stable,
non-empty principal authorized for each action and operation resource. The
bounded `POST /operations/{id}/reconcile` control accepts version, attempt,
full-width fencing proof, `succeeded`, `retry`, or `failed` resolution, actor,
and reason. Principals and actors are limited to 255 bytes and reasons to 4 KiB.
The actor must exactly match the authorized principal; the handler adds server
time and passes a root `ReconcileRequest` to the controller. Operation path
resources must satisfy the lowercase 255-byte identifier grammar before
authorization is invoked.
