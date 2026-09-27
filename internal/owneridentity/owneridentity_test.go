package owneridentity_test

import (
	sequencer "github.com/faustbrian/go-sequencer/v2"
	"github.com/faustbrian/go-sequencer/v2/internal/owneridentity"
	"testing"
)

func TestIdentityIsPreservedOrRejected(t *testing.T) {
	for _, test := range []struct {
		owner string
		valid bool
	}{{"worker-01", true}, {"worker name", true}, {"worker password=synthetic-value", false}, {"worker\x00name", false}, {"", false}, {"\x00\t", false}} {
		if got := owneridentity.Valid(test.owner, sequencer.DefaultMaxActorBytes, sequencer.SanitizePersistenceText); got != test.valid {
			t.Fatalf("owner validity=%t want=%t", got, test.valid)
		}
	}
}
