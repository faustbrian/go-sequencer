# Performance

Planning is proportional to operations plus dependency edges, with sorting for
stable tie-breaking. The default limits are 10,000 operations, 256 direct
dependencies, and depth 1,024.

PostgreSQL claim throughput depends on eligible-index selectivity, transaction
latency, dependency fan-out, and contention. Keep transactions short. Handler
work happens outside the claim transaction and must finish before lease expiry.

Run `make benchmark` on release hardware. Record Go version, CPU, database
version, candidate count, dependency shape, and concurrency. Benchmarks are
capacity evidence, not universal service-level objectives.
