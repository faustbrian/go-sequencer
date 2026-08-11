# Operation lifecycle

Registration projects a new definition from pending to eligible. A replica
claims eligible work, receives a monotonically increasing fencing token, marks
the attempt running, and completes it as succeeded, skipped, failed, retryable,
deferred, blocked, canceled, or dead-lettered. Lease expiry moves claimed or
running work to indeterminate before any replay decision.

Retryable and deferred records become eligible only at their declared instant.
A one-time success is not executed again by an ordinary run. A second explicit
synchronous execution of a `Repeatable` operation first writes an attributed
reset using the runner owner, then claims a new fenced attempt. Other succeeded,
failed, blocked, canceled, and dead-lettered records may be replayed only by an
explicit reset with actor and reason, or a new version. Indeterminate records
require exact reconciliation instead. Definition drift on an existing ID and
version fails closed.

Every attempt remains visible. Audit events record state boundaries,
ownership, fencing, actor, reason, and time. Partial reports do not erase
allowed failures or dead letters. `rolled_back` is a legacy readable state;
current compensation is a separate operation and never means the database
returned to a historical snapshot.

Long-running `Fleet` runners have a separate process lifecycle: `starting`,
`accepting`, `draining`, `stopped`, and `failed`. Only `accepting` is ready or
allowed to initiate a claim. Cancellation moves to `draining` before accepted
handler contexts are canceled. A normal stop means every accepted worker and
lease-renewal goroutine ended; failure preserves the durability error or
shutdown timeout that prevented that claim.

`CancellationCooperative` is the default. It asks a handler to stop during
drain but does not prove an external effect stopped. `CancellationDrainOnly`
withholds shutdown cancellation when interruption is unsafe. If it exceeds the
bounded shutdown wait, the fleet fails, renewal stops, and Kubernetes must end
the pod. Lease recovery records an indeterminate result and does not replay it
unless the registered policy explicitly authorizes idempotent replay.
