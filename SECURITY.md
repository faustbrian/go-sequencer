# Security policy

## Supported versions

| Version | Supported |
| --- | --- |
| 1.1.x | Yes |
| Earlier releases | No |

Security fixes are released from the latest supported minor line. Consumers
should upgrade to its newest patch before reporting a vulnerability.

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
