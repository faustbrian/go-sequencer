# Transactions

`WithinTransaction` means one worker attempt executes inside one local
transaction supplied by `TransactionManager`. The handler receives that
transaction through `Attempt.Transaction`; the sequencer never discovers a
repository or session globally.

The manager must invoke its callback exactly once with a non-nil context and
transaction and must preserve the callback error. Omission or invalid input is
a definite contract failure before handler effects. Repeated invocation,
swallowed callback errors, or a manager panic after callback acceptance is an
unknown result; the runner blocks replay instead of reporting a false success
or invoking the handler twice.

The ledger claim and completion transactions are always distinct from the
application transaction in the current API. Supplying a transaction manager
or arranging for the same database session does not make claim, handler data,
and completion one atomic transaction: the store owns claim and completion,
while `TransactionManager` owns only the handler callback. A process crash
after application commit but before ledger completion is therefore an unknown
result and requires idempotency or an attributed reconciliation decision.

An asynchronous operation cannot share a live transaction with the enqueueing
process, its ledger claim or completion, or later operations. Use an
application outbox when enqueue must follow application data atomically.
Cross-process and cross-operation atomicity are never claimed.
