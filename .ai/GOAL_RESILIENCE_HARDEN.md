# Goal: Harden Sequencer Fleet Resilience

## Non-Negotiable Quality Gate

The module MUST maintain exactly 100% statement coverage and exactly 100% of
viable mutants killed by meaningful tests. Tests MUST prove behavior rather
than merely execute lines or preserve implementation structure.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in BCP 14
[RFC2119] [RFC8174] when, and only when, they appear in all capitals, as
shown here.

## Required Proof

- Kill runners before and after claim, heartbeat, side effect, commit, ledger
  transition, queue settlement, lease release, and shutdown deadline.
- Race drain, claim, renewal, cancellation, completion, takeover, reset, and
  mixed-version registry changes.
- Prove stale generations cannot complete or compensate newer ownership.
- Exercise uncooperative handlers and preserve unknown outcomes honestly.
- Simulate scale-up, rolling update/rollback, scale-down, pod suspension,
  database failover, queue redelivery, and abrupt loss.
- Prove bounded work and one shared retry/hedge amplification budget.

Meaningful exact 100% statement coverage, exactly 100% viable mutation kills,
model, race, fuzz, fault, leak, PostgreSQL/queue, Kubernetes, benchmark, API,
docs, security, and supply-chain gates MUST pass.

[RFC2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC8174]: https://www.rfc-editor.org/rfc/rfc8174
