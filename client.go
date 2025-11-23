// Package poutbox provides a client for enqueuing jobs into a Postgres-backed outbox system.
package poutbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"poutbox/postgres"
)

type Client struct {
	db *sql.DB
}

func NewClient(db *sql.DB) *Client {
	return &Client{
		db: db,
	}
}

type enqueueOptions struct {
	tx         *sql.Tx
	scheduleAt time.Time
}

type EnqueueOption func(*enqueueOptions)

func WithScheduleAt(t time.Time) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.scheduleAt = t
	}
}

func WithTx(tx *sql.Tx) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.tx = tx
	}
}

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
		return queries.EnqueueImmediate(ctx, executor, payload)
	} else {
		return queries.EnqueueScheduled(ctx, executor, postgres.EnqueueScheduledParams{
			Payload:     payload,
			ScheduledAt: opts.scheduleAt,
		})
	}
}
