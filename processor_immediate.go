package poutbox

import (
	"context"
	"log"
	"time"

	"poutbox/postgres"
)

type immediateJobProcessor struct {
	c                   *Consumer
	cursorTime          time.Time
	cursorID            int64
	cursorTransactionID int64
}

//nolint:unused
func (p *immediateJobProcessor) fetch(ctx context.Context) ([]postgres.PoutboxImmediate, error) {
	lookbackTime := p.cursorTime.Add(-1 * time.Hour)
	return p.c.queries.GetImmediateJobs(ctx, p.c.db, postgres.GetImmediateJobsParams{
		LastProcessedTransactionID: p.cursorTransactionID,
		LastProcessedID:            p.cursorID,
		LastProcessedAt:            lookbackTime,
		BatchSize:                  p.c.config.BatchSize,
	})
}

//nolint:unused
func (p *immediateJobProcessor) toHandlerJobs(jobs []postgres.PoutboxImmediate) []HandlerJob {
	handlerJobs := make([]HandlerJob, len(jobs))
	for i, ij := range jobs {
		handlerJobs[i] = HandlerJob{
			ID:      ij.ID,
			Payload: []byte(ij.Payload),
		}
	}
	return handlerJobs
}

//nolint:unused
func (p *immediateJobProcessor) processResults(jobs []postgres.PoutboxImmediate, failedSet map[int64]bool) *ProcessResult {
	failedBatch := &jobBatch{}
	var (
		lastProcessedID            int64
		lastProcessedTime          time.Time
		lastProcessedTransactionID int64
		lastCommitLSN              string
	)

	if len(jobs) > 0 {
		lastJob := jobs[len(jobs)-1]
		lastProcessedID = lastJob.ID
		lastProcessedTime = lastJob.CreatedAt
		lastProcessedTransactionID = lastJob.TransactionID
		lastCommitLSN = lastJob.CommitLsn

		for _, ij := range jobs {
			if failedSet[ij.ID] {
				failedBatch.add(ij.ID, []byte(ij.Payload), 1)
			}
		}
	} else {
		lastProcessedID = p.cursorID
		lastProcessedTime = p.cursorTime
		lastProcessedTransactionID = p.cursorTransactionID
	}

	cursorParams := &postgres.UpdateCursorParams{
		LastProcessedID:            lastProcessedID,
		LastProcessedAt:            lastProcessedTime,
		LastProcessedTransactionID: lastProcessedTransactionID,
		LastLsn:                    lastCommitLSN,
	}

	return &ProcessResult{
		DeadLetter: nil,
		ToRetry:    failedBatch,
		ToDelete:   nil,
		Cursor:     cursorParams,
	}
}

//nolint:unused
func (p *immediateJobProcessor) commit(ctx context.Context, result *ProcessResult) error {
	if (result.ToRetry == nil || len(result.ToRetry.jobs) == 0) && result.Cursor == nil {
		return nil
	}

	tx, err := p.c.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("failed to begin transaction: %v", err)
		return err
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if result.ToRetry != nil && len(result.ToRetry.jobs) > 0 {
		err = p.c.queries.InsertFailedBatch(ctx, tx, postgres.InsertFailedBatchParams{
			Ids:           result.ToRetry.ids(),
			Payloads:      result.ToRetry.payloadsString(),
			ErrorMessages: make([]string, len(result.ToRetry.jobs)),
			RetryCounts:   result.ToRetry.retries(),
		})
		if err != nil {
			log.Printf("failed to insert failed jobs batch: %v", err)
			return err
		}
	}

	if result.Cursor != nil {
		err = p.c.queries.UpdateCursor(ctx, tx, *result.Cursor)
		if err != nil {
			log.Printf("failed to update cursor: %v", err)
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	p.updateCursor(result.Cursor)

	return nil
}

//nolint:unused
func (p *immediateJobProcessor) updateCursor(cursor *postgres.UpdateCursorParams) {
	if cursor != nil {
		p.cursorTime = cursor.LastProcessedAt
		p.cursorID = cursor.LastProcessedID
		p.cursorTransactionID = cursor.LastProcessedTransactionID
	}
}
