package poutbox

import (
	"context"
	"log"

	"poutbox/postgres"
)

type scheduledJobProcessor struct {
	c *Consumer
}

//nolint:unused // marked as unused due to generics
func (p *scheduledJobProcessor) fetch(ctx context.Context) ([]postgres.GetScheduledJobsReadyRow, error) {
	jobs, err := p.c.queries.GetScheduledJobsReady(ctx, p.c.db, p.c.config.BatchSize)
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

//nolint:unused // marked as unused due to generics
func (p *scheduledJobProcessor) toHandlerJobs(jobs []postgres.GetScheduledJobsReadyRow) []HandlerJob {
	handlerJobs := make([]HandlerJob, len(jobs))
	for i, sj := range jobs {
		handlerJobs[i] = HandlerJob{
			ID:      sj.ID,
			Payload: []byte(sj.Payload),
		}
	}

	return handlerJobs
}

//nolint:unused // marked as unused due to generics
func (p *scheduledJobProcessor) processResults(jobs []postgres.GetScheduledJobsReadyRow, failedSet map[int64]bool) *ProcessResult {
	failedBatch := &jobBatch{}
	var toDelete []int64

	for _, sj := range jobs {
		toDelete = append(toDelete, sj.ID)
		if failedSet[sj.ID] {
			failedBatch.add(sj.ID, []byte(sj.Payload), 1)
		}
	}

	return &ProcessResult{
		DeadLetter: nil,
		ToRetry:    failedBatch,
		ToDelete:   toDelete,
		Cursor:     nil,
	}
}

//nolint:unused // marked as unused due to generics
func (p *scheduledJobProcessor) commit(ctx context.Context, result *ProcessResult) error {
	if (result.ToRetry == nil || len(result.ToRetry.jobs) == 0) && len(result.ToDelete) == 0 {
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

	if len(result.ToDelete) > 0 {
		err = p.c.queries.DeleteScheduledBatch(ctx, tx, result.ToDelete)
		if err != nil {
			log.Printf("failed to delete scheduled jobs batch: %v", err)
			return err
		}
	}

	return tx.Commit()
}
