# Hardening Goal: Durable Operation Sequencing

## Objective

Prove operation ordering, persistence, ownership, crash recovery, retries,
transactions, asynchronous execution, and administrative controls correct
under concurrency and failure.

## Required Audits

- Exhaust graph cycles, missing dependencies, duplicate IDs, version/checksum
  drift, channels, conditions, skips, allowed failures, and deterministic order.
- Model-check every state transition and reject illegal/stale transitions.
- Race multiple Kubernetes replicas claiming and completing operations.
- Inject crashes before/during/after claim, transaction, side effect, commit,
  queue acknowledgement, ledger update, and lease expiry.
- Prove fencing and idempotency prevent stale owners from authorizing work.
- Verify async attempts never claim cross-process transaction atomicity.
- Exercise retries, timeouts, dead letters, manual resume/reset, compensation,
  cancellation, and unknown outcomes.
- Verify migration prerequisites and data-before-schema-change deployment
  recipes against real PostgreSQL.
- Fuzz persisted records, plans, graphs, commands, tags, and error codecs.
- Mutation-test state guards, dependency eligibility, ownership proofs,
  checksum checks, and terminal-result aggregation.
- Benchmark large histories, plans, claims, contention, polling, and recovery.

## Release Blockers

- Duplicate unauthorized execution, skipped dependency, unstable order, stale
  owner success, lost ledger transition, transaction lie, infinite retry,
  unrecoverable crash window, race, leak, or unsafe administrative action.

## Completion Criteria

- State, graph, PostgreSQL, crash, failover, queue, lease, idempotency, retry,
  fuzz, race, mutation, and benchmark suites pass with meaningful 100%
  coverage.

