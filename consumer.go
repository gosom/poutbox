package poutbox

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/gosom/poutbox/postgres"
)

// HandlerJob represents a job to be processed by a handler.
type HandlerJob struct {
	// ID is the unique identifier of the job.
	ID int64
	// Payload is the job data in bytes (typically JSON).
	Payload []byte
}

// Handler processes jobs from the outbox.
// It returns the IDs of jobs that failed and should be retried.
type Handler interface {
	// Handle processes a batch of jobs and returns the IDs of failed jobs.
	Handle(ctx context.Context, jobs []HandlerJob) []int64
	// Close closes the handler and releases resources.
	Close(ctx context.Context) error
}

// ConsumerConfig configures job processing behavior.
type ConsumerConfig struct {
	// BatchSize is the number of jobs to fetch and process in a single batch.
	BatchSize int32
	// MaxRetries is the maximum number of times a failed job will be retried.
	MaxRetries int32
	// PollInterval is the duration to wait between polling for new jobs.
	PollInterval time.Duration
	// UseLogicalReplication enables Postgres logical replication for immediate jobs.
	UseLogicalReplication bool
	// UpdateCursorOnLogicalRepl tracks cursor position during logical replication.
	UpdateCursorOnLogicalRepl bool
}

// Consumer processes jobs from the outbox.
// It manages immediate, scheduled, and failed job processing.
type Consumer struct {
	// db is the Postgres database connection.
	db *sql.DB
	// config holds consumer configuration.
	config ConsumerConfig
	// handler processes batches of jobs.
	handler Handler
	// queries provides database query execution.
	queries *postgres.Queries
	// replConnStr is the connection string for logical replication.
	replConnStr string
}

// NewConsumer creates a new Consumer with the given database, handler, and config.
// Sets default values for BatchSize (1000), MaxRetries (3), and PollInterval (100ms).
func NewConsumer(db *sql.DB, handler Handler, config ConsumerConfig) *Consumer {
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 100 * time.Millisecond
	}

	return &Consumer{
		db:      db,
		handler: handler,
		config:  config,
		queries: postgres.New(),
	}
}

// SetReplicationConnString sets the connection string for logical replication.
// Required when UseLogicalReplication is enabled.
func (c *Consumer) SetReplicationConnString(connStr string) {
	c.replConnStr = connStr
}

// Start begins processing jobs from immediate, scheduled, and failed queues.
// It runs three concurrent processors and returns the first error encountered.
func (c *Consumer) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 3)

	wg.Go(func() {
		if err := c.processFailed(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				errChan <- err
			}
		}
	})

	wg.Go(func() {
		var err error
		if c.config.UseLogicalReplication {
			err = c.processImmediateLogicalRepl(ctx)
		} else {
			err = c.processImmediate(ctx)
		}
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				errChan <- err
			}
		}
	})

	wg.Go(func() {
		if err := c.processScheduled(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				errChan <- err
			}
		}
	})

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// Close closes the consumer and releases resources via the handler.
func (c *Consumer) Close(ctx context.Context) error {
	return c.handler.Close(ctx)
}

//nolint:unused // marked as unused due to generics
type job struct {
	id      int64
	payload []byte
	retry   int32
}

type jobBatch struct {
	jobs []job //nolint:unused // marked as unused due to generics
}

// ProcessResult holds the results of processing a batch of jobs.
// It tracks which jobs to retry, delete, or send to dead letter, and updates cursor position.
type ProcessResult struct {
	// DeadLetter contains jobs that exceeded max retries.
	DeadLetter *jobBatch
	// ToRetry contains jobs that failed and should be retried.
	ToRetry *jobBatch
	// ToDelete contains job IDs that were processed successfully.
	ToDelete []int64
	// Cursor tracks the latest processed job ID, timestamp, and transaction ID for resumption.
	Cursor *postgres.UpdateCursorParams
}

//nolint:unused // marked as unused due to generics
func (b *jobBatch) add(id int64, payload []byte, retry int32) {
	b.jobs = append(b.jobs, job{id, payload, retry})
}

//nolint:unused // marked as unused due to generics
func (b *jobBatch) ids() []int64 {
	result := make([]int64, len(b.jobs))
	for i, j := range b.jobs {
		result[i] = j.id
	}
	return result
}

//nolint:unused // marked as unused due to generics
func (b *jobBatch) payloadsString() []string {
	result := make([]string, len(b.jobs))
	for i, j := range b.jobs {
		result[i] = string(j.payload)
	}
	return result
}

//nolint:unused // marked as unused due to generics
func (b *jobBatch) retries() []int32 {
	result := make([]int32, len(b.jobs))
	for i, j := range b.jobs {
		result[i] = j.retry
	}
	return result
}

func (c *Consumer) sleepOrDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.config.PollInterval):
		return nil
	}
}

type jobProcessor[T any] interface {
	fetch(ctx context.Context) ([]T, error)
	toHandlerJobs(jobs []T) []HandlerJob
	processResults(jobs []T, failedSet map[int64]bool) *ProcessResult
	commit(ctx context.Context, result *ProcessResult) error
}

func processJobs[T any, P jobProcessor[T]](ctx context.Context, c *Consumer, proc P) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		jobs, err := proc.fetch(ctx)
		if err != nil {
			log.Printf("failed to fetch %T jobs: %v", proc, err)
			if err := c.sleepOrDone(ctx); err != nil {
				return err
			}

			continue
		}

		if len(jobs) == 0 {
			if err := c.sleepOrDone(ctx); err != nil {
				return err
			}

			continue
		}

		handlerJobs := proc.toHandlerJobs(jobs)

		failedIDs := c.handler.Handle(ctx, handlerJobs)
		failedSet := make(map[int64]bool, len(failedIDs))
		for _, id := range failedIDs {
			failedSet[id] = true
		}

		result := proc.processResults(jobs, failedSet)

		if err := proc.commit(ctx, result); err != nil {
			log.Printf("commit failed: %v", err)

			continue
		}
	}
}

func (c *Consumer) processFailed(ctx context.Context) error {
	return processJobs(ctx, c, &failedJobProcessor{c: c})
}

func (c *Consumer) processImmediate(ctx context.Context) error {
	cursor, err := c.queries.GetCursor(ctx, c.db)
	if err != nil {
		return err
	}

	return processJobs(ctx, c, &immediateJobProcessor{
		c:                   c,
		cursorTime:          cursor.LastProcessedAt,
		cursorID:            cursor.LastProcessedID,
		cursorTransactionID: cursor.LastProcessedTransactionID,
	})
}

func (c *Consumer) processScheduled(ctx context.Context) error {
	return processJobs(ctx, c, &scheduledJobProcessor{c: c})
}
