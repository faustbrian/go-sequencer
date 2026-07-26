package sequencer_test

import (
	"context"
	"fmt"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
)

func ExampleRunner() {
	spec := sequencer.OperationSpec{
		ID: "postal.normalize", Version: 1, Checksum: "sha256:reviewed",
		Description: "Normalize postcodes", Channel: "deploy",
		Policy: sequencer.Policy{Mode: sequencer.OneTime, MaxAttempts: 1, MaxExceptions: 1, Timeout: time.Minute},
		Handler: sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			return sequencer.Output{Summary: "normalized"}, nil
		}),
	}
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	runner, _ := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "example"})
	report, _ := runner.Execute(context.Background())
	fmt.Println(report.Result, report.Operations[0].State)
	// Output: 1 succeeded
}
