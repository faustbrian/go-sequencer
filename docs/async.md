# Asynchronous execution

`goqueue.Dispatcher` publishes operation ID, version, checksum, and a delivery
identity. It does not serialize handler payloads, dependencies, transactions,
or secrets. The application adapts this narrow publisher to queue.

Queue delivery is at least once. `goqueue.Worker` delegates every delivery to
a durable executor; the ledger decides whether the operation is eligible and
who owns the attempt. Redelivery must never bypass checksum or fencing checks.

Enqueue success only proves durable queue admission. Worker success is a later
transaction. Use an application outbox when enqueue must follow another local
database write atomically.
