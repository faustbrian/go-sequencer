# Quickstart

Install the stable root module:

```sh
go get github.com/faustbrian/go-sequencer
```

Save this complete program as `main.go`, then run it with `go run .`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	sequencer "github.com/faustbrian/go-sequencer"
	"github.com/faustbrian/go-sequencer/memory"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	operation := sequencer.OperationSpec{
		ID:          "postal.normalize-postcodes",
		Version:     1,
		Checksum:    "sha256:reviewed-source-checksum",
		Description: "Normalize stored postcode spelling",
		Channel:     "deploy",
		Policy: sequencer.Policy{
			Mode:          sequencer.OneTime,
			MaxAttempts:   1,
			MaxExceptions: 1,
			Timeout:       30 * time.Second,
		},
		Handler: sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			return sequencer.Output{Summary: "normalized postcodes"}, nil
		}),
	}

	plan, err := sequencer.CompilePlan(
		[]sequencer.OperationSpec{operation},
		sequencer.PlanOptions{},
	)
	if err != nil {
		return fmt.Errorf("compile plan: %w", err)
	}

	runner, err := sequencer.NewRunner(
		plan,
		memory.New(),
		sequencer.RunnerOptions{Owner: "quickstart"},
	)
	if err != nil {
		return fmt.Errorf("construct runner: %w", err)
	}

	report, err := runner.Execute(ctx)
	if err != nil {
		return fmt.Errorf("execute plan: %w", err)
	}
	fmt.Println(report.Result, report.Operations[0].State)
	return nil
}
```

The program prints:

```text
1 succeeded
```

For production, install the PostgreSQL schema returned by
`postgres.Migrations()` through the application's migration runner. Construct
`postgres.Store` with a caller-owned pool, inject application dependencies into
concrete handlers, and give every runner or fleet replica a unique stable
owner. Compile the complete plan before workers start, execute it under a
bounded context, and retain and alert on the complete report.

Use `memory.New()` and `sequencertest.NewClock` for deterministic tests. Never
use the memory adapter as a multi-process production ledger.
