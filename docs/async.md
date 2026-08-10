# Asynchronous execution

`goqueue.Dispatcher` publishes operation ID, version, checksum, and a delivery
identity. It does not serialize handler payloads, dependencies, transactions,
or secrets. The application adapts this narrow publisher to queue.

A publisher error is always an unknown admission outcome. `Dispatch` returns
the generated message together with `ErrPublishOutcomeUnknown`, preserving the
delivery identity for transport reconciliation instead of generating an
uncorrelated retry.

Queue delivery is at least once. `goqueue.Worker` delegates every delivery to
a durable executor; the ledger decides whether the operation is eligible and
who owns the attempt. Redelivery must never bypass checksum or fencing checks.

Use `Worker.HandleDelivery` with a delivery-bound settlement. Confirmed durable
completion is acknowledged, definite execution failure is rejected, and
`sequencer.ErrUnknownResult` is left unsettled for redelivery. Failed
acknowledgement or rejection is also unsettled because the queue disposition
was not confirmed. The returned disposition is authoritative; transports must
not infer settlement from the returned error alone.

Enqueue success only proves durable queue admission. Worker success is a later
transaction. Use an application outbox when enqueue must follow another local
database write atomically.
