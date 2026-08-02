# Goal: Sequencer Fleet Resilience

## Non-Negotiable Quality Gate

The module MUST maintain exactly 100% statement coverage and exactly 100% of
viable mutants killed by meaningful tests. Tests MUST prove behavior rather
than merely execute lines or preserve implementation structure.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in BCP 14
[RFC2119] [RFC8174] when, and only when, they appear in all capitals, as
shown here.

## Objective

Complete `sequencer` lifecycle and recovery semantics for horizontally scaled
Kubernetes runners executing long-lived, ordered, and potentially ambiguous
operations.

## Required Scope

- Explicit runner states for starting, accepting, draining, stopped, and failed.
- SIGTERM stops new claims before canceling or draining owned attempts.
- Heartbeat and lease renewal stop in a documented order; lease release MUST not
  imply an uncooperative side effect stopped.
- Stale owners cannot commit completion after takeover because every
  ownership-sensitive write uses fencing.
- Define operation behavior when context cancellation is unsafe or impossible.
- Bound claim polling, concurrency, retries, compensation, dead-letter work,
  history, and shutdown wait.
- Define mixed-binary operation registry/checksum behavior during rolling
  deployments and rollback.
- Compose retry, breaker, bulkhead, adaptive policies, idempotency, lease,
  queue, and shared budgets without duplicate retry loops.

## Kubernetes Semantics

Document replicas, readiness, leaderless claiming, rollout compatibility,
scale-up contention, scale-down drain, pod suspension, SIGTERM, abrupt kill,
termination-grace expiry, PostgreSQL failover, queue ambiguity, and operational
recovery. Kubernetes ownership MUST NOT be described as exactly-once execution.

## Acceptance Criteria

Every accepted attempt has a durable, observable recovery outcome at every
crash point. Meaningful exact 100% statement coverage, exactly 100% viable
mutation kills, model, race, fuzz, fault, leak, real-backend, Kubernetes,
benchmark, API, docs, and security gates MUST pass.

[RFC2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC8174]: https://www.rfc-editor.org/rfc/rfc8174
