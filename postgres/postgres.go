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
