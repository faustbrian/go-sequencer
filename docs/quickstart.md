# Quickstart

1. Define every operation in code with a stable ID, monotonically meaningful
   version, reviewed checksum, explicit dependency list, and finite policy.
2. Compile the complete plan during application startup or deployment
   inspection. Compilation must succeed before workers or handlers run.
3. Install the PostgreSQL schema from `postgres.Migrations()` through the
   application's migration runner.
4. Construct `postgres.Store`, inject application dependencies into concrete
   handlers, and construct a runner with a unique replica owner.
5. Inspect the immutable plan, then execute it under a bounded deployment
   context. Persist and alert on the complete report.

Use `memory.New()` and `sequencertest.NewClock` for deterministic tests. Never
use the memory adapter as a multi-process production ledger.
