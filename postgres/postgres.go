// Package postgres provides database operations for the poutbox outbox system.
// It handles connection management, schema migration, and logical replication for processing jobs.
//
// Example usage with polling mode:
//
//	db, err := postgres.Connect(ctx, connStr)
//	defer db.Close()
//
//	err = postgres.Migrate(ctx, db)
//
// Example usage with logical replication mode:
//
//	db, err := postgres.Connect(ctx, connStr)
//	defer db.Close()
//
//	err = postgres.Migrate(ctx, db)
//	err = postgres.InitializeLogicalReplication(ctx, db)
//
//	stream, err := postgres.NewReplicationStream(ctx, replConnStr, 0)
//	defer stream.Close(ctx)
//
//	for event, err := range stream.Events(ctx) {
//		if err != nil {
//			break
//		}
//		// Process event.Change or event.Keepalive
//	}
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.sql
var schemaFS embed.FS

// Connect establishes a connection to Postgres using the given connection string.
// Configures connection pooling: 25 max open, 5 idle, 5min lifetime, 2min idle timeout.
func Connect(ctx context.Context, connStr string) (*sql.DB, error) {
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)

	const dbPingTimeout = time.Second * 5

	pingCtx, cancel := context.WithTimeout(ctx, dbPingTimeout)
	defer cancel()

	if err = db.PingContext(pingCtx); err != nil {
		return nil, err
	}

	return db, nil
}

// Migrate creates the database schema from the embedded schema.sql file.
func Migrate(ctx context.Context, db *sql.DB) error {
	schemaContent, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("reading schema.sql: %w", err)
	}

	if _, err := db.ExecContext(ctx, string(schemaContent)); err != nil {
		return fmt.Errorf("executing schema: %w", err)
	}

	return nil
}
