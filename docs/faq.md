# FAQ

## Is this a migration engine?

No. Schema history and execution belong to migrations.

## Does one plan run in one transaction?

No. At most one synchronous attempt uses one injected local transaction.

## Are asynchronous operations exactly once?

No. Delivery is at least once; durable claims, fencing, and idempotency make
redelivery safe for correctly designed handlers.

## Does Kubernetes make operation execution exactly once?

No. Pods, leases, and queue deliveries can disappear at ambiguous boundaries.
The ledger records unknown outcomes, fencing rejects stale completion after
takeover, and handlers still need idempotency or reconciliation.

## What happens when cancellation cannot stop an operation?

Declare `CancellationDrainOnly`. SIGTERM stops claims but does not cancel that
handler. If it outlives the shutdown bound, the fleet fails, renewal stops, and
the pod must terminate. Lease expiry records an unknown result; lease release
or pod deletion is never evidence that the side effect stopped.

## Can I change code without changing the version?

Only if the reviewed checksum remains identical. Drift fails closed. Usually a
behavior change needs a new version.

## Why not discover handlers automatically?

Explicit construction keeps dependencies reviewable, testable, bounded, and
free from package-level mutable state.
