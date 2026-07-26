# Security policy

Report vulnerabilities privately to the repository owner. Do not include live
credentials, customer data, operation payloads, or production ledger rows.

The sequencer stores bounded summaries, not arbitrary handler errors, payloads,
stack traces, or secrets. Applications must redact output metadata before
returning it. Administrative inspect, execute, and reset endpoints must remain
behind application-owned authentication, authorization, rate limits, and audit
controls. The HTTP adapter refuses construction without an authorizer.

Treat checksums, operation code, migration prerequisites, reset approvals, and
fencing tokens as integrity-sensitive. A stale owner must never write protected
resources. See [the security guide](docs/security.md).
