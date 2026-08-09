package sequencer

import (
	"errors"
	"testing"
	"time"
)

func TestFleetDrainRetainsFirstWorkerFailure(t *testing.T) {
	t.Parallel()

	cause := errors.New("first completion failed")
	second := errors.New("second completion failed")
	results := make(chan error, 2)
	results <- cause
	results <- second
	fleet := &Fleet{
		options: FleetOptions{ShutdownWait: time.Second},
		state:   RunnerDraining,
	}

	if err := fleet.waitForDrain(results, 2); !errors.Is(err, cause) {
		t.Fatalf("waitForDrain() error = %v", err)
	}
	if fleet.State() != RunnerFailed {
		t.Fatalf("state = %s, want failed", fleet.State())
	}
}
