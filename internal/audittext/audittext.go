// Package audittext prepares attributable administrative text for both stores.
//
// Empty raw attribution is left empty; callers decide when attribution is required.
// Pattern redaction is defensive and does not recognize arbitrary secret formats.
package audittext

import sequencer "github.com/faustbrian/go-sequencer/v2"

// Prepare bounds raw input before redaction and rejects nonempty input that
// becomes empty after sanitization. Initially empty fields remain allowed here.
func Prepare(actor, reason string) (string, string, error) {
	if len(actor) > sequencer.DefaultMaxActorBytes || len(reason) > sequencer.DefaultMaxReasonBytes {
		return "", "", sequencer.ErrResourceLimit
	}
	rawActor, rawReason := actor, reason
	actor = sequencer.SanitizePersistenceText(actor, sequencer.DefaultMaxActorBytes)
	reason = sequencer.SanitizePersistenceText(reason, sequencer.DefaultMaxReasonBytes)
	if rawActor != "" && actor == "" || rawReason != "" && reason == "" {
		return "", "", sequencer.ErrResourceLimit
	}
	return actor, reason, nil
}
