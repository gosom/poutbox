package poutbox

import (
	"context"

	"poutbox/postgres"
)

type failedJobProcessor struct {
	c *Consumer
}

//nolint:unused // marked as unused due to generics
func (p *failedJobProcessor) fetch(ctx context.Context) ([]postgres.PoutboxFailed, error) {
	return p.c.queries.GetFailedJobsReady(ctx, p.c.db, p.c.config.BatchSize)
}

//nolint:unused // marked as unused due to generics
func (p *failedJobProcessor) toHandlerJobs(jobs []postgres.PoutboxFailed) []HandlerJob {
	handlerJobs := make([]HandlerJob, len(jobs))
	for i, fj := range jobs {
		handlerJobs[i] = HandlerJob{
			ID:      fj.ID,
			Payload: fj.Payload,
		}
	}
	return handlerJobs
}

//nolint:unused // marked as unused due to generics
func (p *failedJobProcessor) processResults(jobs []postgres.PoutboxFailed, failedSet map[int64]bool) (*jobBatch, *jobBatch, []int64, *postgres.UpdateCursorParams) {
	deadLetter := &jobBatch{}
	toRetry := &jobBatch{}
	var toDelete []int64

	for _, fj := range jobs {
		if failedSet[fj.ID] {
			nextRetry := fj.RetryCount + 1
			if nextRetry >= p.c.config.MaxRetries {
				deadLetter.add(fj.ID, fj.Payload, nextRetry)
			} else {
				toRetry.add(fj.ID, fj.Payload, nextRetry)
			}
		} else {
			toDelete = append(toDelete, fj.ID)
		}
	}

	return deadLetter, toRetry, toDelete, nil
}

//nolint:unused // marked as unused due to generics
func (p *failedJobProcessor) shouldCommit(deadLetter *jobBatch, toRetry *jobBatch, toDelete []int64, cursor *postgres.UpdateCursorParams) bool {
	return (deadLetter != nil && len(deadLetter.jobs) > 0) ||
		(toRetry != nil && len(toRetry.jobs) > 0) ||
		len(toDelete) > 0
}

//nolint:unused // marked as unused due to generics
func (p *failedJobProcessor) updateCursor(_ *postgres.UpdateCursorParams) {}
