package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// PartitionOps manages table partitions for the immediate queue.
// Creates new partitions and removes old ones based on time windows.
type PartitionOps struct {
	// db is the database connection for executing DDL operations.
	db *sql.DB
	// queries provides database query execution.
	queries *Queries
}

// NewPartitionOps creates a new PartitionOps instance with the given database.
func NewPartitionOps(db *sql.DB) *PartitionOps {
	return &PartitionOps{
		db: db, queries: New(),
	}
}

// Run creates partitions from 'from' to 'to' with the given interval.
// Drops partitions with end time before cutoffTime.
// All operations are wrapped in a transaction with locking for safety.
func (p *PartitionOps) Run(ctx context.Context, from time.Time, to time.Time, interval time.Duration, cutoffTime time.Time) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `SELECT 1 FROM "poutbox".partition_meta FOR UPDATE`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("acquiring lock: %w", err)
	}

	if err := p.createPartitions(ctx, tx, from, to, interval); err != nil {
		_ = tx.Rollback()
		return err
	}

	toDrop, err := p.dropOldPartitions(ctx, tx, cutoffTime)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	var errs []error
	if len(toDrop) > 0 {
		for _, partName := range toDrop {
			dropSQL := fmt.Sprintf(`DROP TABLE IF EXISTS "poutbox".%s`, partName)
			if _, err := p.db.ExecContext(ctx, dropSQL); err != nil {
				errs = append(errs, fmt.Errorf("dropping partition table %s: %w", partName, err))
			}
		}
	}

	return errors.Join(errs...)
}

func (p *PartitionOps) createPartitions(ctx context.Context, tx *sql.Tx, from time.Time, to time.Time, interval time.Duration) error {
	queries := New()

	for t := from; t.Before(to); t = t.Add(interval) {
		tUTC := t.UTC()
		rangeEnd := t.Add(interval)
		rangeEndUTC := rangeEnd.UTC()

		partName := fmt.Sprintf("immediate_%s", tUTC.Format("2006_01_02_15"))

		createSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS "poutbox".%s PARTITION OF "poutbox".immediate
			FOR VALUES FROM ('%s') TO ('%s')
		`, partName, tUTC.Format(time.RFC3339), rangeEndUTC.Format(time.RFC3339))

		if _, err := tx.ExecContext(ctx, createSQL); err != nil {
			return fmt.Errorf("creating partition: %w", err)
		}

		if err := queries.InsertPartitionMeta(ctx, tx, InsertPartitionMetaParams{
			PartitionName: partName,
			RangeStart:    tUTC,
			RangeEnd:      rangeEndUTC,
		}); err != nil {
			return fmt.Errorf("inserting partition meta: %w", err)
		}
	}

	return nil
}

func (p *PartitionOps) dropOldPartitions(ctx context.Context, tx *sql.Tx, cutoffTime time.Time) ([]string, error) {
	partitions, err := p.queries.ListPartitions(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("listing partitions: %w", err)
	}

	var toDrop []string

	for _, part := range partitions {
		if part.RangeEnd.Before(cutoffTime) {
			detachSQL := fmt.Sprintf(`ALTER TABLE "poutbox".immediate DETACH PARTITION "poutbox".%s`, part.PartitionName)
			if _, err := tx.ExecContext(ctx, detachSQL); err != nil {
				return nil, fmt.Errorf("detaching partition %s: %w", part.PartitionName, err)
			}

			if err := p.queries.DeletePartitionMeta(ctx, tx, part.PartitionName); err != nil {
				return nil, fmt.Errorf("deleting partition meta %s: %w", part.PartitionName, err)
			}

			toDrop = append(toDrop, part.PartitionName)
		}
	}

	return toDrop, nil
}
