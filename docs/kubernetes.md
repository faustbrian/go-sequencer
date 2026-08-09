# Kubernetes fleet operation

## Ownership model

Run one `Fleet` per pod with an owner value unique to that pod instance.
Replicas claim directly from PostgreSQL with row locking; there is no leader,
coordinator election, or process-global registry. More replicas raise claim
contention but do not change the concurrency bound inside each pod.

Kubernetes ownership is not exactly-once execution. A pod can lose its lease
after an external effect commits but before ledger completion commits. Recovery
records that attempt as retryable with an unknown result. The next owner gets a
higher fencing token and must reconcile or use an idempotency key before
repeating an ambiguous effect.

## Probes and startup

Constructing a fleet leaves it `starting` and not ready. `Run` first registers
the exact ID, version, and checksum set compiled into that binary. It becomes
`accepting` and ready only after registration succeeds. Checksum drift, store
failure, or ownership loss moves it to `failed` and readiness stays false. Use
`Fleet.Ready` for readiness. Liveness should report process health rather than
restart a healthy pod merely because there is no eligible work.

## Scaling and rollout

- **Replicas:** ready replicas claim leaderlessly. PostgreSQL fencing is
  authoritative; pod names and Kubernetes leases are not operation fencing.
- **Scale up:** new pods register before readiness, then contend through
  `SKIP LOCKED`. Per-pod `MaxConcurrency` and database capacity set the safe
  replica count.
- **Rolling update:** each pod submits exact claim candidates from its local
  plan. An old pod can claim only versions and checksums it can execute. A new
  version is a new ledger identity. Reusing a version with a changed checksum
  fails registration and readiness.
- **Rollback:** an older binary may resume only definitions whose exact
  checksum still matches. Operations introduced only by the newer binary stay
  durable and unclaimed by the old binary. Roll back code and schema only when
  those definitions and dependencies remain compatible.
- **Mixed registries:** binaries may have additive local registries during a
  rollout. Removing or renaming definitions requires an explicit compatibility
  plan; absence from one pod never deletes ledger data.

## Termination sequence

Treat cancellation of `Fleet.Run` as SIGTERM. The owned order is:

1. move from `accepting` to `draining`, making readiness false;
2. stop initiating claims;
3. deliver cancellation only to `CancellationCooperative` handlers;
4. continue fenced renewal while accepted attempts settle;
5. stop each renewal loop before recording that attempt's completion;
6. after `ShutdownWait`, stop remaining renewals and return
   `ErrShutdownTimeout` in the `failed` state.

`CancellationDrainOnly` handlers do not receive SIGTERM cancellation. If such
a handler exceeds the shutdown bound, Go cannot stop its goroutine or prove an
external effect stopped. The pod must be terminated. The lease is not released
as a claim of safety; it expires, recovery records an unknown result, and any
later completion from the stale fencing token is rejected after takeover.

Set `terminationGracePeriodSeconds` longer than `ShutdownWait` plus probe and
runtime overhead. Scale-down should send SIGTERM and wait for drain. An abrupt
kill or termination-grace expiry skips local cleanup; durable lease expiry is
the recovery boundary.

## Suspension and infrastructure ambiguity

A suspended pod cannot renew. Its lease expires even if an uncooperative
external call later resumes. Every ownership-sensitive protected write must
therefore check fencing, and non-fenceable effects require durable idempotency
or reconciliation.

During PostgreSQL failover, runners fail closed if recovery, claim, renewal, or
completion cannot be confirmed. An ambiguous commit is not retried as though it
failed. After recovery, a runner records expired attempts as unknown before
takeover.

Queue acknowledgement is separate from ledger completion. A lost
acknowledgement may redeliver the same message; the worker validates ID,
version, and checksum and delegates ownership to the ledger. Acknowledge only
after the durable executor returns. Redelivery never authorizes a second retry
loop or bypasses the shared execution budget.

## Operational recovery

For every unknown attempt, inspect ledger history, audit fencing, the external
system's idempotency record, and application evidence. Then let the
higher-fenced owner reconcile and continue, perform an attributable reset, or
block the operation. Never infer success from lease release, pod deletion,
queue acknowledgement, or Kubernetes job completion alone.
