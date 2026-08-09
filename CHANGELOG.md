# Changelog

## Unreleased

### Changed

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

### Fixed

- Discard failed handler output so partial or secret-bearing data cannot enter
  durable attempt history.
- Treat cancellation racing registration or claim polling as graceful drain
  while preserving fail-closed behavior for durability failures.
- Preserve accepted owner and fencing details in completion and lease-recovery
  audit events, and reject memory-store ownership writes at lease expiry.

- Reject administrative inspect versions that exceed the platform `uint`
  range instead of narrowing them before controller dispatch.

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
