# Migration integration

`postgres.Migrations()` exposes the sequencer ledger schema as an `fs.FS`.
Applications apply it with migrations or their existing migration runner.
The package never runs schema changes during import or construction.

The `migrations.Bridge` reads the application's current schema version and
asserts an operation prerequisite. It cannot discover, apply, roll back,
baseline, or mutate migration history.

When an operation sits between schema changes, deploy the first schema phase,
assert its version, execute and verify the operation, then deploy the second
schema phase.

## Pinned-dependency migration

Apply `00002_pin_dependency_definitions.sql` as an expand migration before
deploying binaries that persist exact dependency references. It initializes
dependency-free rows to an empty reference set. Existing rows with legacy
ID-only dependencies remain unresolved (`dependency_refs IS NULL`) and cannot
be claimed. Existing rows also retain a NULL channel until deployment writes
the reviewed exact channel; registration never treats NULL and an empty or
non-empty channel as compatible.

Use these deployment phases:

1. **Expand:** apply the forward migrations while old binaries remain schema
   compatible. Migration `00003_block_legacy_unknown_recovery.sql` makes an old
   recovery path fail closed if it tries to replay a blocked unknown outcome.
   Do not introduce new dependency versions or begin a mixed-binary operation
   rollout until every claiming binary uses exact references.
2. **Replace:** replace old runners before enabling blocked unknown outcomes.
   An old runner may fail and restart when it encounters a protected row; do
   not bypass the database fence or infer that the ambiguous effect is safe to
   replay.
3. **Data:** write each reviewed legacy channel explicitly, then register each
   reviewed current definition with the exact dependency ID, version, and
   checksum. Registration may resolve a legacy dependency row only when its
   checksum, channel, and canonical dependency IDs match; never infer historical
   channels, versions, or checksums from current defaults or whichever
   dependency version is newest.
4. **Prove:** require `SELECT count(*) FROM sequencer_operations WHERE
   dependency_refs IS NULL` to return zero, then verify every pinned dependency
   identity exists with its expected checksum before allowing contract work.
5. **Enforce:** apply a later forward contract migration making
   `dependency_refs` mandatory only after the proof passes. Stop the deployment
   on any unresolved or drifting row; do not mark it resolved manually.

For data-before-schema changes, keep old and new application versions compatible
with the expanded schema, execute the pinned backfill, verify its ledger and
data result, and only then apply the incompatible constraint, drop, rename, or
type change. Roll back application code before the contract phase; after the
contract phase, recovery is a new forward migration rather than an inferred
ledger rewrite.

The embedded ledger migrations are forward-only. Recovery from a bad schema
change is a reviewed forward repair or restore; a generic down migration must
not drop operation, attempt, or audit history.
