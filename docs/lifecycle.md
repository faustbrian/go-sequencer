# Operation lifecycle

Registration projects a new definition from pending to eligible. A replica
claims eligible work and receives a monotonically increasing fencing token. An
attempt with remaining execution budget is marked running immediately before
handler execution, then completes as succeeded, skipped, failed, retryable,
deferred, blocked, canceled, or dead-lettered. A recovered claim whose shared
budget is already exhausted settles directly from claimed to failed or
dead-lettered; it never emits a running boundary because no handler starts.
Lease expiry moves claimed or running work to indeterminate before any replay
decision.

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
allowed failures or dead letters. Owner identities are rejected before claiming
if persistence sanitization would change their exact bytes; credential-bearing
or control-bearing identities are never rewritten into colliding fencing names.
Both runner variants use one shared fenced renewal keeper from accepted claim
through pre-handler work, callback cancellation acknowledgement, and bounded
settlement. An initial renewal proves ownership before callback admission.
Synchronous runners now require `LeaseStore` capability. Store implementations
must honor their supplied contexts; renewal loss cancels execution and settlement
and fails closed. Every keeper stops boundedly after settlement; an unresponsive
callback never receives indefinite lease renewal or a replacement execution slot.
Fleet attempts retain exactly their own keeper, without a second renewal worker.
Observer enqueue and drop never wait for callback delivery; consumers needing
an observation must synchronize explicitly with its asynchronous delivery.
`rolled_back` is a legacy readable state;
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
The operation timeout covers approval, condition, transaction, and handler
callbacks together in one retained execution slot. If any callback ignores that
deadline, a late return is persisted as indeterminate, never as a definite
failure or success, because the external effect may have committed. A fleet
then fails closed without admitting replacement work into the retained handler
slot. The process manager must terminate that instance before it can resume
work; an in-process Go callback cannot be preempted safely. The runner waits at
most `HandlerStopWait` after cancellation for a cooperative return before
classifying the callback as still active. An approval that returns after its
deadline cannot start a handler. Approval panics are contained as indeterminate
outcomes without exposing panic values.
Transaction-manager contexts retain their values and own cancellation while
also inheriting the attempt deadline and cancellation. A transaction callback
offered after cancellation cannot start operation work.
