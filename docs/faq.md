# FAQ

## Is this a migration engine?

No. Schema history and execution belong to migrations.

## Does one plan run in one transaction?

No. At most one synchronous attempt uses one injected local transaction.

## Are asynchronous operations exactly once?

No. Delivery is at least once; durable claims, fencing, and idempotency make
redelivery safe for correctly designed handlers.

## Can I change code without changing the version?

Only if the reviewed checksum remains identical. Drift fails closed. Usually a
behavior change needs a new version.

## Why not discover handlers automatically?

Explicit construction keeps dependencies reviewable, testable, bounded, and
free from package-level mutable state.
