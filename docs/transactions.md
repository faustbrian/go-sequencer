# Transactions

`WithinTransaction` means one worker attempt executes inside one local
transaction supplied by `TransactionManager`. The handler receives that
transaction through `Attempt.Transaction`; the sequencer never discovers a
repository or session globally.

The ledger claim transaction and an application data transaction are distinct
unless an application deliberately provides one database session adapter that
can coordinate both. A process crash after application commit but before
ledger completion is an unknown result and requires idempotency or operator
reconciliation.

An asynchronous operation cannot share a live transaction with the enqueueing
process or later operations. Cross-operation atomicity is never claimed.
