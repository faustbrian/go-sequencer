package audittext_test

import (
	"errors"
	"strings"
	"testing"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	"github.com/faustbrian/go-sequencer/v2/internal/audittext"
)

func TestPrepareBoundsAndCleansAttribution(t *testing.T) {
	actor, reason, err := audittext.Prepare("operator\x00 name", "approved token=synthetic-value")
	if err != nil || actor != "operator name" || reason != "approved token=[REDACTED]" {
		t.Fatalf("prepared attribution=%q/%q error=%v", actor, reason, err)
	}
	for _, values := range [][2]string{{"\x00\t", "approved"}, {"owner", "\x00\t"}, {strings.Repeat("a", sequencer.DefaultMaxActorBytes+1), "approved"}, {"owner", strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1)}} {
		if _, _, err := audittext.Prepare(values[0], values[1]); !errors.Is(err, sequencer.ErrResourceLimit) {
			t.Fatalf("invalid attribution error=%v", err)
		}
	}
}
