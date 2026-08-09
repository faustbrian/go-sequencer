# Deployment sequencing

Treat a deployment as explicit phases: apply prerequisite schema migrations,
assert schema versions, inspect the operation plan, execute synchronous work,
release application code, and dispatch asynchronous work where appropriate.

Schema migrations remain in migrations. Operations may be placed between
schema phases by the deployment system, but the sequencer does not own that
system or its migration history.

Synchronous administrative execution may use one coordinator, but Kubernetes
fleets are leaderless. Every pod registers its exact local ID, version, and
checksum candidates before readiness. During a rolling deployment, old pods
claim only old executable versions and new pods claim only their local
versions. Additive registries are supported; checksum drift for the same ID and
version fails closed.

Rollback is safe only when the older binary still carries matching checksums
and compatible schema and dependencies. Definitions absent from the rollback
binary remain in the ledger and are not claimed or deleted. See
[Kubernetes fleet operation](kubernetes.md) for scale and termination details.
