# Operations guide

Monitor counts by state, oldest eligible age, active lease age, retry volume,
unknown results, blocked operations, checksum drift, and partial reports. Alert
on expired running attempts and dependency-blocked deployment phases.

Before reset, inspect definition checksum, complete attempt history, audit
events, external effects, and current schema version. Record the authenticated
actor, reason, ticket, and expected outcome. Reset changes eligibility; it does
not erase history.

Retention must preserve the current projection and the audit period required
by the application. Archive before pruning. Inspection requests are bounded to
10,000 records and administrative endpoints should add tighter product limits.
