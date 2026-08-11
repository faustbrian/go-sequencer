# Changelog

## Unreleased

### Changed

- Fail closed when cancellation races an unknown durable result, reject claims
  for a different local generation before handler execution, and add a
  PostgreSQL fence that prevents older binaries from replaying blocked unknown
  outcomes during rolling updates or rollback.
- Add executable scale-up, scale-down, suspended-container takeover, lifecycle
  race, stale-compensation, and shared retry/hedge-budget resilience proofs.
- Fail readiness immediately when lease ownership is lost even if a handler
  ignores cancellation, and recover expired leases in deterministic batches
  that bound PostgreSQL transactions and in-memory critical sections.
- Preserve ambiguous drain-only timeout outcomes as indeterminate, prevent
  recovered attempts from invoking handlers or reporting a running transition
  beyond `MaxAttempts`, require exact unrounded package coverage, and include
  real PostgreSQL restart and benchmark evidence in resilience verification.
- Block expired claimed or running work as `indeterminate` by default;
  automatic replay now requires an explicit idempotent unknown-outcome policy,
  while manual reconciliation is attributed and bound to the exact attempt and
  fencing token.
- Persist terminal dead-letter state, allow attributed resume of canceled and
  dead-lettered operations, and model compensation as a separate operation
  tied to an exact dependency definition. Legacy `rolled_back` records remain
  readable but receive no new transitions.
- Clarify that the current API cannot make ledger claim or completion atomic
  with an application transaction, even when adapters use one database
  session; asynchronous execution remains cross-process and non-atomic.
- Persist exact dependency ID, version, and checksum references in PostgreSQL;
  deployment must apply the forward dependency expansion, resolve legacy rows
  from reviewed definitions, prove no unresolved rows remain, then enforce.
- Make embedded ledger migrations forward-only so a generic rollback cannot
  drop operation, attempt, and audit history.
- Persist channels, compensation links, unknown-outcome policy, and dead-letter
  policy in PostgreSQL; expired attempts now remain indeterminate unless the
  definition explicitly authorizes idempotent replay, with fenced attributable
  reconciliation for manual outcomes.
- Bound idempotency completion/failure and fenced-lease release calls with
  cancellation-detached cleanup deadlines. Existing constructors use a
  five-second default; callers may select a validated bound up to one minute.
  Unconfirmed cleanup is classified as an unknown result so replay cannot be
  authorized from an ordinary failure.
- Preserve queue delivery identity when publish admission is unknown, and add
  explicit acknowledgement, rejection, and unsettled worker dispositions.
- Add leaderless fleet lifecycle, readiness, fenced lease renewal, bounded
  drain, exact mixed-binary claim candidates, and explicit cooperative versus
  drain-only cancellation semantics.
- Require inline retry adapters to consume one shared execution budget, apply
  the lower independent attempt or exception bound, and keep durable and inline
  retry ownership explicit. Callers of `goretry.Adapter.Do` must now pass the
  `Attempt.Budget` supplied by an inline-mode handler.
- Document Kubernetes rollout, rollback, scale, suspension, termination,
  database failover, queue ambiguity, and unknown-result recovery semantics.
- Delegate local mutation checks to the canonical exact-100 repository runner
  instead of a reduced package-local efficacy threshold.
- Bound PostgreSQL dependency-definition JSON to 64 KiB on writes and reads,
  and fail closed when persisted compensation references exceed 4 KiB.
  Individual definition checksums are limited to 512 bytes, so newly encoded
  compensation references remain below the persisted-record bound.

### Fixed

- Close claim admission atomically when lease ownership fails, and cancel a
  claim returned across that boundary without invoking its handler.
- Treat persisted skips as completed one-time outcomes on later synchronous
  runs instead of attempting an impossible second claim.
- Align the public transition model with audited claimed-attempt recovery and
  attributable successful-operation resets.
- Discard failed handler output so partial or secret-bearing data cannot enter
  durable attempt history.
- Treat cancellation racing registration or claim polling as graceful drain
  while preserving fail-closed behavior for durability failures.
- Preserve accepted owner and fencing details in completion and lease-recovery
  audit events, and reject memory-store ownership writes at lease expiry.
- Reject administrative inspect versions that exceed the platform `uint`
  range instead of narrowing them before controller dispatch.
- Fail closed when a synchronous runner cannot inspect the registered durable
  record instead of proceeding to claim with unknown ledger state.
- Bound reset and reconciliation actors and reasons, reject oversized
  authorized principals, and prevent memory resets from regressing ledger time.
- Bound detached fleet `MarkRunning` persistence by `ShutdownWait` so a stalled
  store cannot leak an accepted worker indefinitely.
- Bound fleet registration, recovery, claim, and renewal calls; a stalled
  store fails the fleet and cancels the accepted attempt before lease expiry.
- Reject malformed or oversized queued operation IDs, checksums, and delivery
  identities before dispatch or execution.
- Persist handler-reported unknown outcomes as `indeterminate` instead of a
  definite failure, and require explicit reconciliation or declared idempotency
  before replay.
- Treat recovered handler panics as indeterminate because the handler may have
  completed an external effect before panicking.
- Execute `Repeatable` synchronous successes through an attributed durable
  reset before the next claim instead of failing with no eligible operation.
- Apply the 64 KiB bound to the complete encoded output in every store so an
  otherwise individually bounded metadata map cannot fail only at PostgreSQL
  completion time.
- Persist independent replay-epoch attempt and typed retry-exception counters;
  reset starts a fresh budget while failover and unknown-result recovery retain
  the current budget instead of conflating it with lifetime attempt history.
- Enforce the transaction-manager exactly-once callback contract and classify
  repeated calls, swallowed callback errors, or post-callback panics as unknown
  instead of allowing duplicate execution or false success.
- Enforce direct store owner, completion-audit, dependency-count, and HTTP
  operation-resource bounds before durable or authorized work, and share the
  same operation-ID grammar across stores, queue, scheduler, and HTTP adapters.
- Bound plan checksums, descriptions, individual tags, and environment
  selectors, and apply the 512-byte checksum limit at direct store and claim
  boundaries before database access.
- Reject a handler's late success after its attempt context has already expired,
  so ignored cancellation cannot turn a declared timeout into durable success.
- Classify PostgreSQL registration commit errors as unknown durable outcomes,
  matching the other transactional ledger writes instead of implying that the
  registration definitely failed.
- Release every non-nil acquired fenced-lease handle, including handles whose
  ownership proof is malformed, so validation failure cannot leak the lease.

### Distribution

- Include the canonical MIT licence in the independently published module.

### Compatibility

- Added a pinned module export baseline so incompatible public API changes
  fail the canonical repository gate.

- Add immutable operation definitions and deterministic dependency plans.
- Add fenced synchronous execution, typed failure handling, audited resets,
  crash recovery, conditional skips, and manual approval.
- Add memory and PostgreSQL ledgers plus queue, scheduler, retry, lease,
  idempotency, migration, HTTP, and testing adapters.
- Add property, fuzz, race, integration, mutation, coverage, and benchmark
  gates.
