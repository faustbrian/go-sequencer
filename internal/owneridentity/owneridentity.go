// Package owneridentity rejects unsafe fencing identities without rewriting them.
//
// Rejecting changed bytes avoids colliding sanitized names in ownership proofs.
package owneridentity

// Valid preserves the exact original identity or rejects it before persistence.
// The caller supplies a non-nil deterministic sanitizer and raw byte bound.
// A nonempty bounded owner is valid only when sanitization leaves its bytes exact.
func Valid(owner string, maximum int, sanitize func(string, int) string) bool {
	return owner != "" && len(owner) <= maximum && sanitize(owner, maximum) == owner
}
