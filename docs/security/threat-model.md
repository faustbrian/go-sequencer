# Threat model

Version: 2.0.0 candidate, reviewed for the security release on 2026-09-27.
Publication, required main CI, and public-consumer verification remain separate
delivery boundaries; this model does not claim they have passed.

## Scope and assets

Sequencer protects durable operation identity, dependency ordering, attempt
ownership, fencing tokens, retry budgets, persisted outcomes, and attributable
administrative decisions. It does not protect application credentials,
business payloads, external side effects, the caller's database account, or
the process host.

## Trust boundaries

- Operation definitions and canonical `sha256:` checksums cross from reviewed
  deployment code into the plan, stores, and queue adapters.
- Handlers, conditions, transaction managers, approvers, and observers are
  application callbacks. They are not trusted to return promptly or avoid
  panics, secret-bearing errors, and oversized output.
- Memory and PostgreSQL stores are persistence boundaries. Stored summaries,
  metadata, errors, actors, and reasons are sanitized and bounded defensively.
- Queue transports are untrusted delivery channels. Messages contain only
  bounded operation identity and a delivery identifier; ledger ownership is
  authoritative.
- `sequencehttp` is an administrative boundary behind caller-owned
  authentication, authorization, rate limiting, TLS, and network policy.

## Threats and controls

| Threat | Control |
| --- | --- |
| Definition substitution or ambiguous checksum text | Canonical lowercase SHA-256 checksum validation and same-version drift rejection |
| Stale or duplicated execution | Durable claims, fencing tokens, lease expiry, and exact completion ownership checks |
| Cancellation-ignoring approval, condition, transaction, or handler callback exhausts workers or renews forever | One attempt deadline covers all callbacks, records an indeterminate result, stops renewal, fails the fleet, and admits no replacement work into the retained execution slot |
| Observer panic, blocking, or secret-bearing errors affect execution | Best-effort single-worker delivery with a bounded queue, panic isolation, and classification-only errors |
| Secrets enter durable summaries or audit text | Default credential-pattern redaction, sensitive metadata-key redaction, and byte limits at runner and store boundaries |
| Administrative inspection causes unbounded adapter work or response bytes | The controller returns pre-encoded JSON; byte length is rejected before bounded validation and headers |
| Queue replay bypasses orchestration | Payload-free messages and ledger-owned eligibility, checksum, and fencing decisions |

## Residual risks and operator obligations

Pattern redaction cannot recognize every application-specific secret. Handlers
must return intentionally non-secret summaries and metadata.

| Accepted risk | Owner | Rationale | Mitigation | Review condition |
| --- | --- | --- | --- | --- |
| Pattern redaction cannot identify every application-specific secret or personal-data field | Deploying application security owner | The library has no application-specific data classification or secret inventory | Return only explicitly non-secret summaries and metadata, mark sensitive metadata keys, and keep payloads outside the ledger | Reassess when new output fields or credential formats are introduced, or application classifications change |
| A callback that ignores cancellation can retain one goroutine and its captured resources until process exit | Deploying application operator | Go cannot safely preempt an arbitrary in-process callback | The timed-out result is indeterminate, its handler slot remains quarantined, fleet admission fails closed, and the process supervisor terminates the instance before restart | Reassess if handlers become untrusted plugins, if process supervision is unavailable, or if a subprocess execution contract is introduced |
| A blocking observer can retain its single delivery goroutine and delay or drop that observer's later events | Deploying application operator | The existing observer API has no cancellation or acknowledgement contract and cannot safely preempt application code | Delivery is isolated from execution, limited to one goroutine and a bounded queue per observer, and errors are reduced to stable classifications | Reassess if lossless telemetry is required, if observers become untrusted plugins, or when a context-aware observer API can be introduced |

Operators must reconcile indeterminate external effects before replay.

The HTTP adapter does not implement identity, rate limits, TLS, CSRF policy, or
network isolation. Applications must supply and test those controls. Database,
queue, and observability credentials remain owned by the application and must
be least-privileged and rotated outside this library.

## Security verification

Security-sensitive changes exercise focused deadline, panic, redaction,
resource-bound, checksum, race, leak, store-integration, API-compatibility,
static-analysis, vulnerability, and secret-scanning checks according to the
affected boundary.
