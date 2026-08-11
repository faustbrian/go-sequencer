# Crash recovery

Recovery uses durable lease expiry and attempt state, never process memory.
An expired claimed or running attempt is recorded as `indeterminate` with an
unknown result. The default `UnknownOutcomeBlock` policy leaves it unclaimable;
it is not an automatic retry.

`UnknownOutcomeReplayIdempotent` explicitly asserts that a durable application
idempotency boundary protects the effect. Only that policy lets lease recovery
audit `indeterminate -> eligible` automatically. `goidempotency` provides an
integration seam; it does not make arbitrary side effects idempotent.

Otherwise inspect the external effect and call `ResolveUnknown` through a
`ReconciliationStore`. The request binds the operation version, exact attempt
number, fencing token, actor, reason, resolution, and non-regressing time. A
stale or repeated decision fails closed. `ReconcileRetry` makes the operation
eligible; `ReconcileSucceeded` records success; `ReconcileFailed` records
`failed` or `dead_lettered` according to policy. Generic reset cannot resolve
an indeterminate result.

Stale owners cannot complete or reset current work. PostgreSQL claim selection
uses row locking with `SKIP LOCKED`, server time, and transactional projection,
attempt, and audit writes.

`goidempotency` terminal updates and `golease` releases detach from caller
cancellation so accepted cleanup still runs, but every call retains an explicit
deadline. `New` uses a five-second bound; `NewWithCleanupTimeout` accepts a
positive bound up to one minute. Cleanup failures retain their underlying cause
and any primary execution failure, but also carry `ErrUnknownResult`; callers
must not authorize replay from an unconfirmed idempotency update or lease
release.

| Crash boundary | Durable recovery outcome |
|---|---|
| before registration or claim | no accepted attempt; eligible work remains |
| after claim, before running | lease expiry records the claimed attempt as indeterminate |
| while the handler or side effect runs | lease expiry records indeterminate; reconciliation or declared idempotency precedes replay |
| after side effect, before completion | indeterminate even if the effect probably committed |
| handler panic before returning an outcome | indeterminate because completed effects cannot be excluded |
| handler reports an unknown result | completion records indeterminate; do not replay from the returned error alone |
| during completion commit | success only when confirmed; otherwise inspect the ledger |
| after completion | the terminal fenced projection and attempt history are authoritative |
| after queue work, before acknowledgement | redelivery consults the ledger; indeterminate work remains unsettled until authorized |

Fleet runners recover expired attempts before claiming. Multiple replicas may
do so concurrently; store serialization and PostgreSQL row locks select the
winner. Each recovery call durably settles at most 32 leases in deterministic
expiry and operation-identity order, so a large backlog cannot create one
unbounded transaction or mutex hold. Subsequent fleet polls settle later
batches. Indeterminate backlog consumes no worker capacity. When policy or
exact reconciliation authorizes replay, the next claim increments the attempt
and fencing token so an old completion remains stale. Renewal extends ownership
only; stopping renewal or clearing a completed lease never asserts that an
uncooperative external effect stopped.
