# Operations guide

Monitor counts by state, oldest eligible age, active lease age, retry volume,
indeterminate and dead-lettered results, blocked or canceled operations,
definition drift, and partial reports. Alert on expired running attempts,
unresolved indeterminate work, dead-letter growth, and dependency-blocked
deployment phases.

Before reset, inspect definition checksum, complete attempt history, audit
events, external effects, and current schema version. Record the authenticated
actor, reason, ticket, and expected outcome. Reset changes eligibility; it does
not erase history. It can resume succeeded, failed, blocked, canceled, or
dead-lettered work, but it cannot resolve indeterminate work.

For indeterminate work, reconcile the external effect first. Submit an exact
attempt number and fencing token with the authenticated actor, reason,
resolution, and a time no earlier than the ledger record. Treat a stale or
ambiguous rejection as a new inspection requirement, never as permission to
retry with broader identity.

Retention must preserve the current projection and the audit period required
by the application. Archive before pruning. Inspection requests are bounded to
10,000 records and administrative endpoints should add tighter product limits.
