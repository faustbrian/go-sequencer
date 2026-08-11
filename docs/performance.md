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

## Reproducible workloads

The in-process suite uses fixed identifiers, checksums, timestamps, graph
shapes, and concurrency. Fixture construction and state preparation are outside
the timed regions. Every workload reports allocations, and scale-sensitive
workloads also report their operation, record, candidate, contender, or expired
record count:

- exact pinned-dependency planning: a 1,000-operation linear graph and a
  10,000-operation, 40-wide layered graph;
- maximum bounded in-memory history and audit reads of 10,000 records;
- a 10,000-candidate claim with the eligible definition last;
- 32 replicas contending for one claim, with exactly one winner checked outside
  the measurement result;
- one fleet initial poll over 10,000 registrations with a deterministic 10%
  channel selection;
- recovery of 1,000 simultaneously expired, replay-idempotent leases;
- confirmed queue settlement and bounded eight-attempt retry and idempotency
  adapter paths.

Run the suite with a fixed sample count when comparing revisions:

```sh
go test ./... -run '^$' -bench . -benchmem -count 10 -benchtime=1s
```

Compare complete raw samples with `benchstat`; do not gate on a single elapsed
time or an uncalibrated workstation threshold. Record `go version`, OS and
architecture, CPU model, power mode, commit, command, and whether the machine
was otherwise idle.

## PostgreSQL methodology

The integration-tagged PostgreSQL benchmarks start the digest-pinned reference
container and apply the owned migrations before timing. Container startup,
migration, registration, history construction, and transition preparation are
excluded; measured work still includes real client/server transactions, row
locking, index scans, and result decoding. The workloads measure a 1,000-entry
candidate claim with the eligible definition last and a 1,000-attempt bounded
history read. Run them separately so container lifecycle does not distort the
in-process sample:

```sh
go test -tags=integration ./postgres -run '^$' -bench Postgres -benchmem \
  -count 10 -benchtime=1s
```

Record the image digest, Docker or container runtime version, host resources,
pool settings, database settings, and whether the database was local or remote.
These workloads compare identical revisions and environments; they intentionally
define no flaky wall-clock pass/fail threshold.
