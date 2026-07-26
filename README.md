# sequencer

`sequencer` is a durable orchestration library for one-time and explicitly
repeatable application operations. It keeps data changes separate from schema
migrations, compiles immutable dependency plans, and records every attempt
under fenced ownership.

The root module contains no global registry, reflection discovery, filesystem
scan, hidden worker, or implicit goroutine. Applications construct operations,
stores, runners, transport adapters, authentication, and dependencies.

```go
operation := sequencer.OperationSpec{
    ID: "postal.normalize-postcodes", Version: 1,
    Checksum: "sha256:reviewed-source-checksum",
    Description: "Normalize stored postcode spelling", Channel: "deploy",
    Policy: sequencer.Policy{
        Mode: sequencer.OneTime, MaxAttempts: 3, MaxExceptions: 3,
        Timeout: time.Minute,
    },
    Handler: sequencer.HandlerFunc(func(ctx context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
        return sequencer.Output{Summary: "normalized postcodes"}, nil
    }),
}
plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{operation}, sequencer.PlanOptions{})
if err != nil { /* fail deployment */ }
runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: replicaID})
if err != nil { /* fail deployment */ }
report, err := runner.Execute(ctx)
```

PostgreSQL is the production reference store. `memory` is a deterministic
reference adapter. `goqueue`, `scheduler`, `goretry`, `golease`, and
`goidempotency` are explicit integration seams. `migrations` asserts schema
prerequisites without owning migration history. `sequencehttp` requires an
application authorizer for every administrative action.

Start with the [quickstart](docs/quickstart.md), then read the
[lifecycle](docs/lifecycle.md), [transaction](docs/transactions.md), and
[recovery](docs/recovery.md) contracts. All documentation is indexed in
[docs/README.md](docs/README.md).

Requires Go 1.26.5. Run `make check` for the complete local gate.
