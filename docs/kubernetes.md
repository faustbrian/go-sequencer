# Kubernetes fleet operation

## Ownership model

Run one `Fleet` per pod with an owner value unique to that pod instance.
Replicas claim directly from PostgreSQL with row locking; there is no leader,
coordinator election, or process-global registry. More replicas raise claim
contention but do not change the concurrency bound inside each pod.

Kubernetes ownership is not exactly-once execution. A pod can lose its lease
after an external effect commits but before ledger completion commits. Recovery
records that attempt as indeterminate and leaves it unclaimable by default. An
exact reconciliation decision or an explicitly declared idempotent-replay
policy is required before a next owner gets a higher fencing token. Stale
completion remains rejected.

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
- **Scale down:** terminating pods close admission and drain accepted work;
  surviving pods claim the remaining eligible operations. Abruptly removed
  owners recover only through lease expiry and fencing.
- **Rolling update:** each pod submits exact claim candidates from its local
  plan. An old pod can claim only versions and checksums it can execute. A new
  version is a new ledger identity. Reusing a version with a changed checksum
  fails registration and readiness.
- **Unknown-outcome compatibility:** apply migration 00003 before a mixed
  rollout that can create blocked unknown outcomes. It makes pre-hardening
  recovery fail closed instead of replaying an ambiguous effect. Old pods may
  fail and restart when they encounter such a row; finish the rollout before
  recovery resumes.
- **Rollback:** an older binary may resume only definitions whose exact
  checksum still matches. Operations introduced only by the newer binary stay
  durable and unclaimed by the old binary. Roll back code and schema only when
  those definitions and dependencies remain compatible.
- **Mixed registries:** binaries may have additive local registries during a
  rollout. Removing or renaming definitions requires an explicit compatibility
  plan; absence from one pod never deletes ledger data.

## Termination sequence

The application must forward process signals into `Fleet.Run`; the package does
not install global signal handlers. A typical entry point uses
`signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)`,
defers the returned stop function, and passes that context to `Run`. Treat that
context cancellation as SIGTERM. The owned order is:

1. move from `accepting` to `draining`, making readiness false;
2. stop initiating claims;
3. deliver cancellation only to `CancellationCooperative` handlers;
4. continue fenced renewal while accepted attempts settle;
5. stop each renewal loop and its `EventHeartbeat` emissions before recording
   that attempt's completion;
6. after `ShutdownWait`, stop remaining renewals and return
   `ErrShutdownTimeout` in the `failed` state.

After `Run` returns, no renewal loop or heartbeat emission owned by that fleet
remains. `ClaimInterval` is finite and cannot exceed one minute.

Registration, recovery, claim, cancellation-detached `MarkRunning`, and final
completion calls are limited by `ShutdownWait`. Each renewal is limited by
the earlier of `ShutdownWait` and the remaining lease window, so a stalled
renewal fails the fleet and cancels the attempt before an unbounded call can
silently outlive ownership. Readiness becomes false and `Run` returns on lease
loss even when a handler ignores cancellation; the process manager must then
terminate that uncooperative handler.

`CancellationDrainOnly` handlers do not receive SIGTERM cancellation. If such
a handler exceeds the shutdown bound, Go cannot stop its goroutine or prove an
external effect stopped. The pod must be terminated. The lease is not released
as a claim of safety; it expires, recovery records an indeterminate result, and
any later completion from the stale fencing token is rejected after an
authorized takeover.

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
failed. After recovery, a runner records expired attempts as indeterminate.
Default policy stops there; an operator must reconcile the exact attempt and
fencing token unless the registered policy explicitly declares durable
idempotency.

Database topology and acknowledged-write durability remain deployment-owned.
Use a multi-host connection string with read-write target selection so the pool
can reject a read-only standby and reconnect after promotion. If acknowledged
ledger writes must survive primary loss, require synchronous replication at
least through remote apply; asynchronous replication can lose an acknowledged
claim, renewal, completion, or audit record and cannot support that guarantee.
The PostgreSQL integration gate kills a primary after a synchronously applied
claim, promotes a different physical standby, proves pool reconnection, records
the expired attempt as indeterminate, rejects stale completion, and verifies a
reconciled takeover receives a higher fence.

Queue acknowledgement is separate from ledger completion. A lost
acknowledgement may redeliver the same message; the worker validates ID,
version, and checksum and delegates ownership to the ledger. Acknowledge only
after the durable executor returns. Redelivery never authorizes a second retry
loop or bypasses the shared execution budget.

## Operational recovery

For every indeterminate attempt, inspect ledger history, audit fencing, the
external system's idempotency record, and application evidence. Resolve that
exact attempt and fencing token as succeeded, failed, or eligible for retry.
Generic reset cannot resolve it. Never infer success from lease release, pod
deletion, queue acknowledgement, or Kubernetes job completion alone.
