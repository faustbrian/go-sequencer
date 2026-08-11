# Rollback and compensation

Rollback handlers are compensating business operations. They cannot undo an
email, external API call, consumed queue message, or committed database write.
Never describe them as database time travel.

Model a compensation as its own reviewed operation with a stable ID, checksum,
bounded policy, and idempotency key. Set `OperationSpec.Compensates` to the
exact ID, version, and checksum of the forward operation and include the same
reference in `DependencyRefs`. This gives the compensation its own durable
attempts, ownership, fencing, retries, and crash semantics. Its result never
rewrites the forward operation's projection.

If a compensation result is unknown, stop automatic progress and reconcile the
external effect before reset. A new forward version is often safer than trying
to reconstruct a historical state.

An attributed reset of the forward operation fails while any related
compensation is claimed, running, retryable, deferred, indeterminate, or
replay-eligible after an earlier attempt. Complete or reconcile that exact
compensation generation first. The PostgreSQL ledger enforces the same rule in
database triggers so an older binary cannot race a compensation claim across a
newer forward generation. A terminal compensation may be reset only while its
original forward fencing generation is still terminal; use a new compensation
version for a later forward generation.

`rolled_back` remains readable for legacy ledgers, but no current state
transition enters it. New compensation must be represented by a separate
operation.
