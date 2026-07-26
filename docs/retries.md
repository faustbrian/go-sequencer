# Retries and dead letters

Every policy declares finite maximum attempts, maximum exceptions, and a
timeout. Only `sequencer.Retry` permits another durable attempt. Untyped errors
and `sequencer.Permanent` fail immediately. Cancellation and deadline outcomes
remain distinct classifications.

Use `goretry` for bounded transient retries inside one owned attempt. Use a new
durable sequencer attempt when the prior attempt boundary must remain visible.
Do not multiply both budgets without calculating their worst-case duration.

Dead-letter policy means exhausted work remains terminal and operator-visible;
it is never deleted or replayed automatically. An audited reset or new version
is required to try it again.
