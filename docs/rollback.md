# Rollback and compensation

Rollback handlers are compensating business operations. They cannot undo an
email, external API call, consumed queue message, or committed database write.
Never describe them as database time travel.

Model a compensation as its own reviewed operation with a stable ID, checksum,
dependencies, bounded policy, and idempotency key. This gives the compensation
the same durable ownership and crash semantics as forward work. Record the
relationship in tags and output metadata.

If a compensation result is unknown, stop automatic progress and reconcile the
external effect before reset. A new forward version is often safer than trying
to reconstruct a historical state.
