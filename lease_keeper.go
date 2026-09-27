package sequencer

import (
	"context"
	"errors"
	"sync"
	"time"
)

// startClaimRenewal owns one existing fenced renewal loop until settlement.
// Initial renewal acknowledges a possibly delayed claim before callback work.
func (fleet *Fleet) startClaimRenewal(parent context.Context, claim Claim, running <-chan struct{}, cancelExecution context.CancelFunc) (func() error, <-chan error) {
	ctx, cancelRenewal := context.WithCancel(parent)
	stopped, failed, firstRenewed := make(chan struct{}), make(chan error, 1), make(chan struct{})
	fleet.renewals.Go(func() {
		fleet.renewLease(ctx, running, cancelExecution, claim.Ownership(), claim.Attempt.Number, claim.Until, stopped, failed, firstRenewed)
	})
	var once sync.Once
	var stopErr error
	stop := func() error {
		once.Do(func() {
			cancelRenewal()
			timer := time.NewTimer(min(fleet.options.ShutdownWait, fleet.options.LeaseDuration))
			defer timer.Stop()
			select {
			case <-stopped:
			case <-timer.C:
				stopErr = ErrShutdownTimeout
			}
		})
		return stopErr
	}
	timer := time.NewTimer(max(0, min(fleet.options.ShutdownWait, claim.Until.Sub(fleet.options.Clock.Now()))))
	defer timer.Stop()
	select {
	case <-firstRenewed:
	case <-stopped:
		if err := ctx.Err(); err != nil {
			select {
			case failed <- err:
			default:
			}
		}
	case <-timer.C:
		cancelExecution()
		select {
		case failed <- errors.Join(ErrTimeout, context.DeadlineExceeded):
		default:
		}
	}
	return stop, failed
}
