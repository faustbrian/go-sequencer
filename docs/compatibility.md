# Compatibility

The published v2 module requires Go 1.27.0 or later. V2.0.0 established the
stable `/v2` module path; v1 remains available from the module path without
that suffix.
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
plans, direct stores, and queue adapters. A persisted `(operation ID, version)`
cannot be converted in place: registering a new checksum for it fails with
`ErrChecksumDrift`. Introduce a new operation version with a canonical checksum
and route new commands to that version. Keep old definitions in the old binary
and ledger while compatible workers drain or reconcile outstanding legacy queue
messages; do not rewrite their version or checksum. Retain the old binary and
queue route for any rollback window that still requires old-version claims.

## Migrating from v1

To adopt the published v2 module, change the required module and every
package import from
`github.com/faustbrian/go-sequencer` to
`github.com/faustbrian/go-sequencer/v2`. Give converted definitions a new
version and `ChecksumBytes` checksum, and dispatch new commands with that exact
version and checksum. Drain or reconcile legacy queued commands with compatible
old workers before retiring their route; v2 workers cannot claim legacy opaque
checksums. Review the new handler cancellation acknowledgement and best-effort
observer delivery contracts before rollout.

The module manifest declares no owned runtime consumers. The released
[go-library-tools v1.8.5 compatibility fixture](https://github.com/faustbrian/go-library-tools/tree/v1.8.5/release/compatibility-consumer)
covers sequencer v1 only; it does not establish v2 compatibility. A v2 release
requires its own clean public-consumer evidence. Retain v1 coverage when
expanding a shared fixture to cover v2.

Each stable v2 release requires a reviewed API baseline, green required CI on
the exact main source, and a successful release rehearsal. Publish a new
immutable version tag, then verify that exact public module through a clean
consumer before delivery is complete. Secret scanning uses the authenticated,
checksum-pinned go-library-tools v1.7.2 contract; local results do not establish
hosted CI or publication.

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
