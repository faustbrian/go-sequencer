# Compatibility

The module targets Go 1.26.6 and tests the current stable Go toolchain in CI.
PostgreSQL 18 is the reference integration target; SQL uses ordinary arrays,
JSONB, row locks, partial indexes, and server timestamps.

Public root interfaces follow semantic versioning. Adding a method to `Store`
is breaking. Optional infrastructure remains in subpackages so root consumers
do not inherit transport dependencies.

Ledger migrations are versioned and reversible for development. Production
rollback must account for retained history and must never drop tables merely to
roll back application code.

Rolling binaries use exact `ClaimCandidate` values. The legacy ID-only claim
surface selects the latest version and is suitable only when every claimant has
one identical registry; fleet runners never use it. Same-version checksum drift
is incompatible and blocks readiness. Add a version for behavior changes and
keep old definitions available until no old pod can claim them and rollback no
longer requires them.

The canonical adapter imports are `adapters/idempotency`, `adapters/lease`,
`adapters/queue`, and `adapters/retry`. The released `goidempotency`, `golease`,
`goqueue`, and `goretry` paths remain supported compatibility facades for the
longer of 180 days after successor publication and two subsequently published
stable root-module minor releases.

Migrate imports and selectors together:

```go
import (
	sequenceridempotency "github.com/faustbrian/go-sequencer/adapters/idempotency"
	sequencerlease "github.com/faustbrian/go-sequencer/adapters/lease"
	sequencerqueue "github.com/faustbrian/go-sequencer/adapters/queue"
	sequencerretry "github.com/faustbrian/go-sequencer/adapters/retry"
)

dispatcher, err := sequencerqueue.NewDispatcher(publisher, "operations")
leaseAdapter, err := sequencerlease.New(acquirer)
idempotencyAdapter, err := sequenceridempotency.New(gate)
retryAdapter, err := sequencerretry.New(policy)
```

The package examples compile these canonical imports and constructors in CI.

The canonical and legacy packages preserve values, sentinels, defaults, and
runtime behavior, but their exported named types intentionally remain distinct
Go types. Change a collaborator's message, ownership, classification, or
adapter types in the same migration rather than assigning a canonical value to
a legacy named type.

`goretry.Adapter.Do` and `adapters/retry.Adapter.Do` require the shared
`Attempt.Budget` as their second
argument. Inline-retry handlers must pass that budget through rather than
constructing an adapter-local retry count; durable-retry handlers should not
call the inline adapter.
