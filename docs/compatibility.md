# Compatibility

The module targets Go 1.26.6 and tests the current stable Go toolchain in CI.
PostgreSQL 18 is the reference integration target; SQL uses ordinary arrays,
JSONB, row locks, partial indexes, and server timestamps.

Public root interfaces follow semantic versioning. Adding a method to `Store`
is breaking. Optional infrastructure remains in subpackages so root consumers
do not inherit transport dependencies.

Ledger migrations are versioned and reversible for development. Production
rollback must account for retained history and must never drop tables merely to
roll back application code.

Rolling binaries use exact `ClaimCandidate` values. The legacy ID-only claim
surface selects the latest version and is suitable only when every claimant has
one identical registry; fleet runners never use it. Same-version checksum drift
is incompatible and blocks readiness. Add a version for behavior changes and
keep old definitions available until no old pod can claim them and rollback no
longer requires them.

`goretry.Adapter.Do` requires the shared `Attempt.Budget` as its second
argument. Inline-retry handlers must pass that budget through rather than
constructing an adapter-local retry count; durable-retry handlers should not
call the inline adapter.
