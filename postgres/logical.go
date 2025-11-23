package postgres

import (
	"context"
	"database/sql"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	PublicationName    = "poutbox_immediate_pub"
	ReplicationSlot    = "poutbox_immediate_slot"
	_ReplicationPlugin = "pgoutput"
)

func InitializeLogicalReplication(ctx context.Context, db *sql.DB) error {
	return withPgxConn(ctx, db, func(ctx context.Context, pgxConn *pgx.Conn) error {
		if err := createPublicationInNewTx(ctx, pgxConn); err != nil {
			return err
		}

		if err := createReplicationSlotInNewTx(ctx, pgxConn); err != nil {
			return err
		}

		return nil
	})
}

func withPgxConn(ctx context.Context, db *sql.DB, fn func(context.Context, *pgx.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	var pgxConn *pgx.Conn
	err = conn.Raw(func(driverConn any) error {
		pgxConn = driverConn.(*stdlib.Conn).Conn()
		return nil
	})
	if err != nil {
		return err
	}

	return fn(ctx, pgxConn)
}

func createPublicationInNewTx(ctx context.Context, pgxConn *pgx.Conn) error {
	tx, err := pgxConn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := createPublicationTx(ctx, tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func createPublicationTx(ctx context.Context, tx pgx.Tx) error {
	var pubExists bool
	err := tx.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", PublicationName).
		Scan(&pubExists)
	if err != nil {
		return err
	}

	if pubExists {
		var pubInsert, pubUpdate, pubDelete, pubTruncate bool
		err := tx.QueryRow(ctx,
			"SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1", PublicationName).
			Scan(&pubInsert, &pubUpdate, &pubDelete, &pubTruncate)
		if err != nil {
			return err
		}

		log.Printf("Publication %s current settings: insert=%v, update=%v, delete=%v, truncate=%v\n",
			PublicationName, pubInsert, pubUpdate, pubDelete, pubTruncate)

		if !pubInsert || pubUpdate || pubDelete || pubTruncate {
			log.Printf("Publication %s has incorrect settings, dropping and recreating...\n", PublicationName)
			_, err = tx.Exec(ctx, `DROP PUBLICATION IF EXISTS `+PublicationName)
			if err != nil {
				return err
			}

			pubExists = false
		}
	}

	if !pubExists {
		_, err = tx.Exec(ctx, `CREATE PUBLICATION `+PublicationName+` FOR TABLE "poutbox"."immediate" WITH (publish='insert')`)
		if err != nil {
			return err
		}

		log.Printf("Created publication %s with INSERT-only configuration\n", PublicationName)
	}

	return nil
}

func createReplicationSlotInNewTx(ctx context.Context, pgxConn *pgx.Conn) error {
	tx, err := pgxConn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := createReplicationSlotTx(ctx, tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func createReplicationSlotTx(ctx context.Context, tx pgx.Tx) error {
	var slotExists bool
	err := tx.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", ReplicationSlot).
		Scan(&slotExists)
	if err != nil {
		return err
	}

	if !slotExists {
		_, err = tx.Exec(ctx,
			"SELECT pg_create_logical_replication_slot($1, $2)", ReplicationSlot, _ReplicationPlugin)
		if err != nil {
			return err
		}

		log.Printf("Created replication slot: %s\n", ReplicationSlot)
	}

	return nil
}
