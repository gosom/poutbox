package poutbox

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sync"
	"time"

	"poutbox/postgres"
)

type HandlerJob struct {
	ID      int64
	Payload []byte
}

type Handler interface {
	Handle(ctx context.Context, jobs []HandlerJob) []int64
	Close(ctx context.Context) error
}

type ConsumerConfig struct {
	BatchSize             int32
	MaxRetries            int32
	PollInterval          time.Duration
	UseLogicalReplication bool
	LogicalReplBatchSize  int
}

type Consumer struct {
	db               *sql.DB
	config           ConsumerConfig
	handler          Handler
	queries          *postgres.Queries
	replConnStr      string
	lastProcessedLSN postgres.LSN
}

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

func (c *Consumer) SetReplicationConnString(connStr string) {
	c.replConnStr = connStr
}

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

type ProcessResult struct {
	DeadLetter *jobBatch
	ToRetry    *jobBatch
	ToDelete   []int64
	Cursor     *postgres.UpdateCursorParams
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
func (b *jobBatch) payloads() [][]byte {
	result := make([][]byte, len(b.jobs))
	for i, j := range b.jobs {
		result[i] = j.payload
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
