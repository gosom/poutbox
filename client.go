// Package poutbox provides a reliable outbox pattern implementation for asynchronous job processing.
// Jobs are stored in Postgres, processed by a consumer, and retried on failure.
//
// Example usage with polling:
//
//	client := poutbox.NewClient(db)
//
//	// Enqueue a job for immediate processing
//	jobID, err := client.Enqueue(ctx, MyJob{Data: "example"})
//
//	// Enqueue a job to be processed at a specific time
//	jobID, err := client.Enqueue(ctx, MyJob{Data: "example"}, poutbox.WithScheduleAt(time.Now().Add(1*time.Hour)))
//
//	// Start processing jobs with polling
//	consumer := poutbox.NewConsumer(db, myHandler, poutbox.ConsumerConfig{
//		BatchSize:             100,
//		MaxRetries:            3,
//		PollInterval:          100 * time.Millisecond,
//		UseLogicalReplication: false,
//	})
//	consumer.Start(ctx)
//
// Example usage with logical replication:
//
//	client := poutbox.NewClient(db)
//
//	jobID, err := client.Enqueue(ctx, MyJob{Data: "example"})
//
//	// Start processing jobs with logical replication (cursor position tracked automatically)
//	// when UpdateCursorOnLogicalRepl is true
//	consumer := poutbox.NewConsumer(db, myHandler, poutbox.ConsumerConfig{
//		BatchSize:             100,
//		MaxRetries:            3,
//		UseLogicalReplication: true,
//		UpdateCursorOnLogicalRepl: true,
//	})
//	consumer.SetReplicationConnString(replConnStr)
//	consumer.Start(ctx)
package poutbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/gosom/poutbox/postgres"
)

// Client enqueues jobs into the Postgres-backed outbox system.
type Client struct {
	// db is the Postgres database connection used to store jobs.
	db *sql.DB
}

// NewClient creates a new Client with the given database connection.
func NewClient(db *sql.DB) *Client {
	return &Client{
		db: db,
	}
}

type enqueueOptions struct {
	tx         *sql.Tx
	scheduleAt time.Time
}

// EnqueueOption is a function that configures job enqueue behavior.
type EnqueueOption func(*enqueueOptions)

// WithScheduleAt schedules a job to be processed at the specified time.
func WithScheduleAt(t time.Time) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.scheduleAt = t
	}
}

// WithTx enqueues a job within an existing database transaction.
func WithTx(tx *sql.Tx) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.tx = tx
	}
}

// Enqueue adds a job to the outbox for processing.
// The job is marshaled to JSON and stored. It returns the job ID.
// Use WithScheduleAt to defer processing, WithTx to enqueue within a transaction.
func (c *Client) Enqueue(ctx context.Context, job any, options ...EnqueueOption) (int64, error) {
	payload, err := json.Marshal(job)
	if err != nil {
		return 0, err
	}

	opts := enqueueOptions{}
	for _, opt := range options {
		opt(&opts)
	}

	var executor postgres.DBTX = c.db
	if opts.tx != nil {
		executor = opts.tx
	}

	queries := postgres.New()

	if opts.scheduleAt.IsZero() {
		return queries.EnqueueImmediate(ctx, executor, string(payload))
	} else {
		return queries.EnqueueScheduled(ctx, executor, postgres.EnqueueScheduledParams{
			Payload:     string(payload),
			ScheduledAt: opts.scheduleAt,
		})
	}
}
