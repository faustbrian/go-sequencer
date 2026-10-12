// Package postgres provides the production reference durable ledger.
//
// The store borrows a caller-owned pgx pool; construction neither installs the
// schema nor starts workers. Apply embedded forward-only migrations through the
// application migration runner. PostgreSQL server time governs claims, renewals
// and completion; leases use whole milliseconds and ledger counters fit bigint.
package postgres

import (
	"embed"
	"io/fs"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrations returns the embedded versioned SQL filesystem without executing it.
// The scripts are forward-only: their Down sections intentionally retain ledger
// history. The application owns ordering and staged prerequisite validation.
func Migrations() fs.FS {
	migrations, _ := fs.Sub(migrationFiles, "migrations")
	return migrations
}
