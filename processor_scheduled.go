package poutbox

import (
	"context"

	"poutbox/postgres"
)

type scheduledJobProcessor struct {
	c *Consumer
}

//nolint:unused // marked as unused due to generics
func (p *scheduledJobProcessor) fetch(ctx context.Context) ([]postgres.GetScheduledJobsReadyRow, error) {
	return p.c.queries.GetScheduledJobsReady(ctx, p.c.db, p.c.config.BatchSize)
}

//nolint:unused // marked as unused due to generics
func (p *scheduledJobProcessor) toHandlerJobs(jobs []postgres.GetScheduledJobsReadyRow) []HandlerJob {
	handlerJobs := make([]HandlerJob, len(jobs))
	for i, sj := range jobs {
		handlerJobs[i] = HandlerJob{
			ID:      sj.ID,
			Payload: sj.Payload,
		}
	}
	return handlerJobs
}

//nolint:unused // marked as unused due to generics
func (p *scheduledJobProcessor) processResults(jobs []postgres.GetScheduledJobsReadyRow, failedSet map[int64]bool) (*jobBatch, *jobBatch, []int64, *postgres.UpdateCursorParams) {
	failedBatch := &jobBatch{}
	var toDelete []int64

	for _, sj := range jobs {
		toDelete = append(toDelete, sj.ID)
		if failedSet[sj.ID] {
			failedBatch.add(sj.ID, sj.Payload, 1)
		}
	}

	return nil, failedBatch, toDelete, nil
}

//nolint:unused // marked as unused due to generics
func (p *scheduledJobProcessor) shouldCommit(deadLetter *jobBatch, toRetry *jobBatch, toDelete []int64, cursor *postgres.UpdateCursorParams) bool {
	return (toRetry != nil && len(toRetry.jobs) > 0) || len(toDelete) > 0
}

//nolint:unused // marked as unused due to generics
func (p *scheduledJobProcessor) updateCursor(_ *postgres.UpdateCursorParams) {}
