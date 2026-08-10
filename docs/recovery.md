# Crash recovery

Recovery uses durable lease expiry and attempt state, never process memory.
An expired claimed or running attempt is recorded as retryable with an unknown
result, then made eligible with a higher future fencing token.

Before retrying an unknown result, the handler must be idempotent or reconcile
whether the protected effect committed. `goidempotency` provides an explicit
integration seam; it does not make arbitrary side effects idempotent.

Stale owners cannot complete or reset current work. PostgreSQL claim selection
uses row locking with `SKIP LOCKED`, server time, and transactional projection,
attempt, and audit writes.

`goidempotency` terminal updates and `golease` releases detach from caller
cancellation so accepted cleanup still runs, but every call retains an explicit
deadline. `New` uses a five-second bound; `NewWithCleanupTimeout` accepts a
positive bound up to one minute. Cleanup failures remain joined with the
primary execution failure.

| Crash boundary | Durable recovery outcome |
|---|---|
| before registration or claim | no accepted attempt; eligible work remains |
| after claim, before running | lease expiry records the claimed attempt as retryable and unknown |
| while the handler or side effect runs | lease expiry records unknown; reconciliation precedes replay |
| after side effect, before completion | unknown even if the effect probably committed |
| during completion commit | success only when confirmed; otherwise inspect the ledger |
| after completion | the terminal fenced projection and attempt history are authoritative |
| after queue work, before acknowledgement | redelivery re-enters ledger ownership and stays fenced |

Fleet runners recover expired attempts before claiming. Multiple replicas may
do so concurrently; store serialization and PostgreSQL row locks select the
winner. A higher-fenced takeover makes an old completion stale. Renewal extends
ownership only; stopping renewal or clearing a completed lease never asserts
that an uncooperative external effect stopped.
