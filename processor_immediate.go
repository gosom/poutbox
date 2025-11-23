package poutbox

import (
	"context"
	"time"

	"poutbox/postgres"
)

type immediateJobProcessor struct {
	c                   *Consumer
	cursorTime          time.Time
	cursorID            int64
	cursorTransactionID int64
}

//nolint:unused // marked as unused due to generics
func (p *immediateJobProcessor) fetch(ctx context.Context) ([]postgres.PoutboxImmediate, error) {
	lookbackTime := p.cursorTime.Add(-1 * time.Hour)
	return p.c.queries.GetImmediateJobs(ctx, p.c.db, postgres.GetImmediateJobsParams{
		LastProcessedTransactionID: p.cursorTransactionID,
		LastProcessedID:            p.cursorID,
		LastProcessedAt:            lookbackTime,
		BatchSize:                  p.c.config.BatchSize,
	})
}

//nolint:unused // marked as unused due to generics
func (p *immediateJobProcessor) toHandlerJobs(jobs []postgres.PoutboxImmediate) []HandlerJob {
	handlerJobs := make([]HandlerJob, len(jobs))
	for i, ij := range jobs {
		handlerJobs[i] = HandlerJob{
			ID:      ij.ID,
			Payload: ij.Payload,
		}
	}
	return handlerJobs
}

//nolint:unused // marked as unused due to generics
func (p *immediateJobProcessor) processResults(jobs []postgres.PoutboxImmediate, failedSet map[int64]bool) (*jobBatch, *jobBatch, []int64, *postgres.UpdateCursorParams) {
	failedBatch := &jobBatch{}
	var (
		lastProcessedID            int64
		lastProcessedTime          time.Time
		lastProcessedTransactionID int64
	)

	if len(jobs) > 0 {
		lastJob := jobs[len(jobs)-1]
		lastProcessedID = lastJob.ID
		lastProcessedTime = lastJob.CreatedAt
		lastProcessedTransactionID = lastJob.TransactionID

		for _, ij := range jobs {
			if failedSet[ij.ID] {
				failedBatch.add(ij.ID, ij.Payload, 1)
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
	}

	return nil, failedBatch, nil, cursorParams
}

//nolint:unused // marked as unused due to generics
func (p *immediateJobProcessor) shouldCommit(deadLetter *jobBatch, toRetry *jobBatch, toDelete []int64, cursor *postgres.UpdateCursorParams) bool {
	return (toRetry != nil && len(toRetry.jobs) > 0) || cursor != nil
}

//nolint:unused // marked as unused due to generics
func (p *immediateJobProcessor) updateCursor(cursor *postgres.UpdateCursorParams) {
	if cursor != nil {
		p.cursorTime = cursor.LastProcessedAt
		p.cursorID = cursor.LastProcessedID
		p.cursorTransactionID = cursor.LastProcessedTransactionID
	}
}
