package sequencer_test

import (
	"errors"
	"testing"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

func TestClassifiedErrorsPreserveCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("temporary outage")
	tests := []struct {
		name string
		err  error
		kind error
	}{
		{"permanent", sequencer.Permanent(cause), sequencer.ErrPermanent},
		{"retryable", sequencer.Retry(cause), sequencer.ErrRetryable},
		{"skip", sequencer.Skip(cause), sequencer.ErrSkipped},
		{"blocked", sequencer.Block(cause), sequencer.ErrBlocked},
		{"unknown", sequencer.UnknownResult(cause), sequencer.ErrUnknownResult},
		{"rollback", sequencer.RollbackFailure(cause), sequencer.ErrRollback},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !errors.Is(test.err, test.kind) || !errors.Is(test.err, cause) {
				t.Fatalf("error %v does not preserve kind and cause", test.err)
			}
		})
	}
	if got := sequencer.Permanent(cause).Error(); got == "" {
		t.Fatal("classified error text is empty")
	}
	if !errors.Is(sequencer.Retry(nil), sequencer.ErrRetryable) {
		t.Fatal("nil cause did not retain classification")
	}
}

func TestSanitizePersistenceTextBoundsAndNormalizes(t *testing.T) {
	t.Parallel()

	got := sequencer.SanitizePersistenceText("token\x00\nvalue", 8)
	if got != "token va" {
		t.Fatalf("SanitizePersistenceText() = %q", got)
	}
	if got := sequencer.SanitizePersistenceText("value", 0); got != "" {
		t.Fatalf("zero-bound value = %q", got)
	}
}
