# Compatibility

The module targets Go 1.26.5 and tests the current stable Go toolchain in CI.
PostgreSQL 18 is the reference integration target; SQL uses ordinary arrays,
JSONB, row locks, partial indexes, and server timestamps.

Public root interfaces follow semantic versioning. Adding a method to `Store`
is breaking. Optional infrastructure remains in subpackages so root consumers
do not inherit transport dependencies.

Ledger migrations are versioned and reversible for development. Production
rollback must account for retained history and must never drop tables merely to
roll back application code.
