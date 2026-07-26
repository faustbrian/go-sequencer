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
