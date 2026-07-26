# Changelog

## Unreleased

### Fixed

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
