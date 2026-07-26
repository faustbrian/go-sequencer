# Cookbook

## One-time data backfill

Define one operation with finite budgets and inject a repository into its
handler. Return only a count summary. Use `WithinTransaction` only when the
entire attempt fits one bounded local transaction.

## Operation after schema version

Call `migrations.Bridge.Assert` from an explicit deployment prerequisite or
condition. Do not let the handler run schema migrations.

## Asynchronous batch

Dispatch an identity-only `goqueue.Message`. In the worker, reload the
definition by ID and version, verify checksum, claim the attempt, and use an
idempotency key derived from operation ID, version, and attempt semantics.

## Manual approval

Set `RequiresApproval` and inject an approver backed by the application's
authenticated change-control system. Denials become blocked audit records.
