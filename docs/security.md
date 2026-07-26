# Security

Operation source and checksums are deployment-controlled code. Never accept
arbitrary operation definitions, handler names, dependencies, or reset commands
from an unauthenticated request.

The runner persists only stable error classifications. Output is limited to a
bounded summary and small string metadata map. Applications must ensure those
fields contain no credentials, personal data, payloads, SQL, stack traces, or
raw upstream errors.

Fencing tokens must accompany writes to protected resources. A lease without
fencing prevents simultaneous ownership but may not prevent a paused stale
process from writing later. Administrative HTTP controls require an injected
authorizer and should also use CSRF protection, rate limits, and request audit.
