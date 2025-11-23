package poutbox

import (
	"context"
	"log"

	"poutbox/postgres"
)

type failedJobProcessor struct {
	c *Consumer
}

//nolint:unused
func (p *failedJobProcessor) fetch(ctx context.Context) ([]postgres.PoutboxFailed, error) {
	return p.c.queries.GetFailedJobsReady(ctx, p.c.db, p.c.config.BatchSize)
}

//nolint:unused
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

//nolint:unused
func (p *failedJobProcessor) processResults(jobs []postgres.PoutboxFailed, failedSet map[int64]bool) *ProcessResult {
	deadLetter := &jobBatch{}
	toRetry := &jobBatch{}

	var toDelete []int64

	for _, fj := range jobs {
		if failedSet[fj.ID] {
			nextRetry := fj.RetryCount + 1
			if nextRetry >= p.c.config.MaxRetries {
				deadLetter.add(fj.ID, fj.Payload, nextRetry)
				toDelete = append(toDelete, fj.ID)
			} else {
				toRetry.add(fj.ID, fj.Payload, nextRetry)
			}
		} else {
			toDelete = append(toDelete, fj.ID)
		}
	}

	return &ProcessResult{
		DeadLetter: deadLetter,
		ToRetry:    toRetry,
		ToDelete:   toDelete,
		Cursor:     nil,
	}
}

//nolint:unused
func (p *failedJobProcessor) commit(ctx context.Context, result *ProcessResult) error {
	if (result.DeadLetter == nil || len(result.DeadLetter.jobs) == 0) &&
		(result.ToRetry == nil || len(result.ToRetry.jobs) == 0) &&
		len(result.ToDelete) == 0 {
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

	if result.DeadLetter != nil && len(result.DeadLetter.jobs) > 0 {
		err = p.c.queries.InsertDeadLetterBatch(ctx, tx, postgres.InsertDeadLetterBatchParams{
			Ids:           result.DeadLetter.ids(),
			Payloads:      result.DeadLetter.payloads(),
			ErrorMessages: make([]string, len(result.DeadLetter.jobs)),
			RetryCounts:   result.DeadLetter.retries(),
		})
		if err != nil {
			log.Printf("failed to insert dead letters batch: %v", err)
			return err
		}
	}

	if result.ToRetry != nil && len(result.ToRetry.jobs) > 0 {
		err = p.c.queries.InsertFailedBatch(ctx, tx, postgres.InsertFailedBatchParams{
			Ids:           result.ToRetry.ids(),
			Payloads:      result.ToRetry.payloads(),
			ErrorMessages: make([]string, len(result.ToRetry.jobs)),
			RetryCounts:   result.ToRetry.retries(),
		})
		if err != nil {
			log.Printf("failed to insert failed jobs batch: %v", err)
			return err
		}
	}

	if len(result.ToDelete) > 0 {
		err = p.c.queries.DeleteFailedBatch(ctx, tx, result.ToDelete)
		if err != nil {
			log.Printf("failed to delete failed jobs batch: %v", err)
			return err
		}
	}

	return tx.Commit()
}
