# Retries and dead letters

Every policy declares finite maximum attempts, maximum exceptions, and a
timeout. Only `sequencer.Retry` permits another durable attempt. Untyped errors
and `sequencer.Permanent` fail immediately. Cancellation and deadline outcomes
remain distinct classifications.

Attempt and exception maxima are independent; either may be lower, and the
lower exhausted bound stops execution. Inline mode gives its adapter that same
lower bound as one shared callback budget.

Durable stores persist a replay-epoch attempt counter and an independent typed
retry-exception counter. `Claim.Budget` carries both values atomically with the
fenced claim, so fleet failover cannot reset or conflate them. An attributed
reset starts a fresh replay epoch; ordinary retry eligibility and unknown-result
reconciliation retain the current counters. A `Retryable` completion must mark
the typed retry exception, while permanent and unknown outcomes cannot spend
that exception budget implicitly.
An unknown-outcome replay claim beyond `MaxAttempts` is durably failed with
`ErrBudgetExhausted` without invoking the handler.

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
`OperationSpec.Compensates` identifies the exact forward `DependencyRef`; that
reference must also be an explicit dependency and never rewrites the forward
operation's state.

With `Policy.DeadLetter`, a permanent, timed-out, or exhausted terminal failure
is stored and reported as `dead_lettered` instead of `failed`. Dead-letter
policy starts no automatic worker: the record remains terminal,
operator-visible, never deleted, and never replayed automatically. An
attributed reset or new version is required to try it again. `AllowedFailure`
may make the aggregate report partial, but it does not hide or change the
durable dead-letter state.
