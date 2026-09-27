// Package owneridentity rejects unsafe fencing identities without rewriting them.
package owneridentity

// Valid preserves the exact original identity or rejects it before persistence.
// The caller supplies its canonical persistence sanitizer and raw byte bound.
func Valid(owner string, maximum int, sanitize func(string, int) string) bool {
	return owner != "" && len(owner) <= maximum && sanitize(owner, maximum) == owner
}
