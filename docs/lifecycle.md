# Operation lifecycle

Registration projects a new definition from pending to eligible. A replica
claims eligible work, receives a monotonically increasing fencing token, marks
the attempt running, and completes it as succeeded, skipped, failed, retryable,
deferred, blocked, or canceled.

Retryable and deferred records become eligible only at their declared instant.
A one-time success is not executed again by an ordinary run. Replay requires
an explicit reset with actor and reason, or a new version. Checksum drift on an
existing ID and version fails closed.

Every attempt remains visible. Audit events record state boundaries,
ownership, fencing, actor, reason, and time. Partial reports do not erase
allowed failures. Rolled back means a declared compensation completed; it does
not mean the database returned to a historical snapshot.
