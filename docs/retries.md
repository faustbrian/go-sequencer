# Retries and dead letters

Every policy declares finite maximum attempts, maximum exceptions, and a
timeout. Only `sequencer.Retry` permits another durable attempt. Untyped errors
and `sequencer.Permanent` fail immediately. Cancellation and deadline outcomes
remain distinct classifications.

Attempt and exception maxima are independent; either may be lower, and the
lower exhausted bound stops execution. Inline mode gives its adapter that same
lower bound as one shared callback budget.

Select exactly one retry owner. `DurableRetries` is the default and creates a
new fenced ledger attempt for each typed `sequencer.Retry`. It does not expose
an inline execution budget. `InlineRetries` creates one durable attempt and
supplies `Attempt.Budget`; `goretry.Adapter` consumes that shared budget before
every callback. It cannot exceed the declared maximum even if an external
policy asks for more calls. Do not construct a second independent retry loop.

Compose one attempt in this order: queue redelivery identifies work; the ledger
claims and fences it; breaker and adaptive admission may reject without
retrying; bulkhead admission bounds local concurrency; idempotency guards the
logical effect; the selected retry owner consumes the shared budget; the
handler performs the effect; completion uses the same fencing proof; only then
may the queue acknowledge. A lease or bulkhead release reports resource
ownership release, not that an uncooperative side effect stopped.

Compensation is a separate operation with its own finite policy and budget.
Dead-letter policy starts no automatic worker: exhausted work remains one
terminal operator-visible record, bounded to an audited reset or separately
declared operation.

Dead-letter policy means exhausted work remains terminal and operator-visible;
it is never deleted or replayed automatically. An audited reset or new version
is required to try it again.
