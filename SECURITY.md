# Security policy

## Supported versions

| Version | Supported |
| --- | --- |
| 2.0.x | After `v2.0.0` is published |
| 1.1.x | Yes |
| Earlier releases | No |

An unpublished v2 candidate is not a supported release. Once published,
security fixes target the newest patch of each supported line. Version 1.1.x
remains supported for existing consumers; ending that support requires a
separate published decision after reviewing their migration horizon.

## Reporting

Report vulnerabilities privately to the repository owner. Do not include live
credentials, customer data, operation payloads, or production ledger rows.
Expect an acknowledgement within three business days. The maintainer will
coordinate validation, remediation, disclosure timing, and a release advisory
through the private report.

The sequencer stores bounded summaries, not arbitrary handler errors, payloads,
stack traces, or secrets. Applications must redact output metadata before
returning it. Administrative inspect, execute, reset, and reconcile endpoints must remain
behind application-owned authentication, authorization, rate limits, and audit
controls. The HTTP adapter refuses construction without an authorizer.

Treat checksums, operation code, migration prerequisites, reset approvals, and
fencing tokens as integrity-sensitive. A stale owner must never write protected
resources. See [the security guide](docs/security.md).
The repository threat model is documented in
[docs/security/threat-model.md](docs/security/threat-model.md).
