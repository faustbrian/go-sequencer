# Deployment sequencing

Treat a deployment as explicit phases: apply prerequisite schema migrations,
assert schema versions, inspect the operation plan, execute synchronous work,
release application code, and dispatch asynchronous work where appropriate.

Schema migrations remain in migrations. Operations may be placed between
schema phases by the deployment system, but the sequencer does not own that
system or its migration history.

Run only one administrative coordinator when possible; ledger fencing remains
mandatory because replicas, retries, and operator races still occur.
