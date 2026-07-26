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
