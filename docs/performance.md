# Performance

Planning is proportional to operations plus dependency edges, with sorting for
stable tie-breaking. The default limits are 10,000 operations, 256 direct
dependencies, and depth 1,024.

PostgreSQL claim throughput depends on eligible-index selectivity, transaction
latency, dependency fan-out, and contention. Keep transactions short. Handler
work happens outside the claim transaction and must finish before lease expiry.

Fleet claim polling is at most once per configured interval while idle and one
candidate is accepted per claim transaction. Per-pod concurrency is hard-capped
at 1,024. Renewal must precede lease expiry; measure database latency and leave
margin for failover. Shutdown is capped at 30 minutes, while each operation's
finite attempts and timeout bound retry and compensation work.

Run `make benchmark` on release hardware. Record Go version, CPU, database
version, candidate count, dependency shape, concurrency, renewal, and recovery
latency under replica contention. Benchmarks are capacity evidence, not
universal service-level objectives.
