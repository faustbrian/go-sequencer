# Compatibility

The stable v2.0.0 candidate requires and tests with Go 1.27.0. It is not installable
until the `v2.0.0` release is published; v1 remains available from the
module path without the `/v2` suffix.
PostgreSQL 18 is the reference integration target; SQL uses ordinary arrays,
JSONB, row locks, partial indexes, and server timestamps.

Public root interfaces follow semantic versioning. Adding a method to `Store`
is breaking. Optional infrastructure remains in subpackages so root consumers
do not inherit transport dependencies.

Definition checksums use the canonical 71-byte lowercase `sha256:` form returned by
`ChecksumBytes`. V2 synchronous runners also require `LeaseStore` capability:
custom wrappers must delegate fenced renewal so leases remain valid through
pre-handler work, callback cancellation acknowledgement, and settlement.
This behavioral requirement is enforced despite the unchanged `Store` parameter.
Legacy opaque checksum strings are rejected consistently by
plans, direct stores, and queue adapters; deployments must update registered
definitions and queued commands together before upgrading.

## Migrating from v1

After v2 publication, change the module and every package import from
`github.com/faustbrian/go-sequencer` to
`github.com/faustbrian/go-sequencer/v2`. Update definitions and queued commands
to use `ChecksumBytes` output before allowing v2 workers to claim them. Review
the new handler cancellation acknowledgement and best-effort observer delivery
contracts before rollout.

The local Golib ecosystem has no owned runtime consumer of sequencer. The
released `go-library-tools/release/compatibility-consumer` fixture is the only
direct consumer found; it remains pinned to v1 until v2 is published, then must
add a distinct v2 compatibility import without replacing its v1 coverage.

Publication requires a reviewed v2 API baseline and green required CI on the
exact main source. The immutable `v2.0.0` tag must then be published and verified
through a clean public consumer before delivery is complete. Secret scanning
uses the authenticated, checksum-pinned go-library-tools v1.7.2 contract;
local results do not establish hosted CI or publication.

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
	sequenceridempotency "github.com/faustbrian/go-sequencer/v2/adapters/idempotency"
	sequencerlease "github.com/faustbrian/go-sequencer/v2/adapters/lease"
	sequencerqueue "github.com/faustbrian/go-sequencer/v2/adapters/queue"
	sequencerretry "github.com/faustbrian/go-sequencer/v2/adapters/retry"
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
