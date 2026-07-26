// Package postgres provides the production reference durable ledger.
package postgres

import (
	"embed"
	"io/fs"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrations returns versioned SQL suitable for migrations or another
// application-owned migration runner.
func Migrations() fs.FS {
	migrations, _ := fs.Sub(migrationFiles, "migrations")
	return migrations
}
