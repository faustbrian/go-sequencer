# Security

Operation source and checksums are deployment-controlled code. Never accept
arbitrary operation definitions, handler names, dependencies, or reset commands
from an unauthenticated request. Plan and direct-store validation still bounds
checksums, descriptions, tags, environments, and dependency definitions so a
misconfigured deployment cannot create unbounded clones or database commands.

The runner persists only stable error classifications. The complete JSON
encoding of an output is limited to 64 KiB in both stores in addition to the
summary, metadata-count, key, and value bounds. Applications must ensure those
fields contain no credentials, personal data, payloads, SQL, stack traces, or
raw upstream errors.

The PostgreSQL adapter rejects dependency-definition JSON larger than 64 KiB
before writing it. Individual 512-byte checksum limits keep newly encoded
compensation references below their 4 KiB persisted-record bound. The adapter
also fails closed when stored definitions exceed either bound or stored attempt
output exceeds the 64 KiB output limit, preventing unbounded JSON decoding of
corrupted ledger records.

Fencing tokens must accompany writes to protected resources. A lease without
fencing prevents simultaneous ownership but may not prevent a paused stale
process from writing later. Administrative HTTP controls require an injected
authorizer and reject principals longer than 255 bytes. Reset and reconciliation
actors are limited to 255 bytes and reasons to 4 KiB. Applications should also
use CSRF protection, rate limits, and request audit.

Unknown-outcome replay is denied by default. Declare
`UnknownOutcomeReplayIdempotent` only when a durable application-owned
idempotency boundary protects the effect. Reconciliation is an administrative
authorization: bind it to the exact version, attempt, and fencing token;
require a bounded authenticated actor and reason; and reject stale timestamps.
Do not use generic reset to bypass an indeterminate result.
