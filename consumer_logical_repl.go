package poutbox

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/gosom/poutbox/postgres"
)

const (
	logicalReplReconnectionWaitTime = 5 * time.Second
)

// processImmediateLogicalRepl starts the logical replication consumer with automatic reconnection
func (c *Consumer) processImmediateLogicalRepl(ctx context.Context) error {
	if err := postgres.InitializeLogicalReplication(ctx, c.db); err != nil {
		return err
	}

	if c.replConnStr == "" {
		return errors.New("replication connection string not set")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := c.runLogicalReplicationSession(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}

			log.Printf("Replication session failed: %v, reconnecting in %v", err, logicalReplReconnectionWaitTime)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(logicalReplReconnectionWaitTime):
				continue
			}
		}
	}
}

func (c *Consumer) runLogicalReplicationSession(ctx context.Context) error {
	batchSize := int(c.config.BatchSize)
	log.Printf("Starting logical replication session with batch size %d", batchSize)

	cursor, err := c.queries.GetCursor(ctx, c.db)
	if err != nil {
		log.Printf("Failed to get cursor: %v", err)
		return err
	}

	if cursor.LastLsn == "" {
		cursor.LastLsn = "0/0"
	}

	stream, err := postgres.NewReplicationStream(ctx, c.replConnStr, cursor.LastLsn)
	if err != nil {
		log.Printf("Failed to create replication stream: %v", err)

		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*3)
		defer cancel()

		_ = stream.Close(closeCtx)
	}()

	buffer := make([]*postgres.LogicalReplChange, 0, batchSize)

	var (
		lastProcessedLSN postgres.LSN
		lastFlushedTime  = time.Now()
	)

	for event, err := range stream.Events(ctx) {
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("Error receiving replication event: %v", err)
			}

			return err
		}

		var keepAliveRequestNeedsReply *postgres.KeepaliveRequest

		switch event.Type {
		case postgres.ReplicationEventTypeInsert:
			buffer = append(buffer, event.Change)
		case postgres.ReplicationEventTypeKeepalive:
			if event.Keepalive.ReplyRequested {
				keepAliveRequestNeedsReply = event.Keepalive
			}
		}

		flushed := false
		elapsed := time.Since(lastFlushedTime)

		if len(buffer) >= batchSize || (elapsed >= c.config.PollInterval && len(buffer) > 0) || (keepAliveRequestNeedsReply != nil && len(buffer) > 0) {
			lastProcessedLSN, err = c.processBatch(ctx, buffer)
			if err != nil {
				log.Printf("Failed to process batch: %v", err)

				return err
			}

			buffer = buffer[:0]
			flushed = true
			lastFlushedTime = time.Now()
		}

		if keepAliveRequestNeedsReply != nil {
			lastProcessedLSN = max(lastProcessedLSN, keepAliveRequestNeedsReply.ServerWALEnd)
			if err := stream.SendKeepalive(ctx, lastProcessedLSN); err != nil {
				log.Printf("Failed to send keepalive response: %v", err)

				return err
			}

			log.Printf("Responded to keepalive request up to LSN %s", lastProcessedLSN)
		} else if flushed {
			if err := stream.SendKeepalive(ctx, lastProcessedLSN); err != nil {
				log.Printf("Failed to send keepalive response: %v", err)

				return err
			}
		}
	}

	return nil
}

func (c *Consumer) processBatch(ctx context.Context, batch []*postgres.LogicalReplChange) (postgres.LSN, error) {
	handlerJobs := make([]HandlerJob, len(batch))
	for i := range batch {
		handlerJobs[i] = HandlerJob{ID: batch[i].ID, Payload: batch[i].Payload}
	}

	failedIDs := c.handler.Handle(ctx, handlerJobs)
	failedSet := c.createFailedSet(failedIDs)

	var (
		maxLSN                           postgres.LSN
		maxChangeByTransactionIDAndIDPos int
	)

	var (
		toRetry         []int64
		toRetryPayloads [][]byte
	)

	for i := range batch {
		if batch[i].LSN > maxLSN {
			maxLSN = batch[i].LSN
		}

		if c.config.UpdateCursorOnLogicalRepl {
			if batch[i].TransactionID > batch[maxChangeByTransactionIDAndIDPos].TransactionID ||
				(batch[i].TransactionID == batch[maxChangeByTransactionIDAndIDPos].TransactionID && batch[i].ID > batch[maxChangeByTransactionIDAndIDPos].ID) {
				maxChangeByTransactionIDAndIDPos = i
			}
		}

		failed := failedSet[batch[i].ID]
		if failed {
			toRetry = append(toRetry, batch[i].ID)
			toRetryPayloads = append(toRetryPayloads, batch[i].Payload)
		}
	}

	if c.config.UpdateCursorOnLogicalRepl || len(toRetry) > 0 {
		tx, err := c.db.BeginTx(ctx, nil)
		defer func() {
			_ = tx.Rollback()
		}()

		if err != nil {
			return 0, err
		}

		if len(toRetry) > 0 {
			if err := c.storeFailedMessages(ctx, tx, toRetry, toRetryPayloads); err != nil {
				log.Printf("failed to store failed messages: %v", err)
				return 0, err
			}
		}

		if c.config.UpdateCursorOnLogicalRepl {
			cursorParams := postgres.UpdateCursorParams{
				LastProcessedID:            batch[maxChangeByTransactionIDAndIDPos].ID,
				LastProcessedAt:            batch[maxChangeByTransactionIDAndIDPos].CreatedAt,
				LastProcessedTransactionID: batch[maxChangeByTransactionIDAndIDPos].TransactionID,
				LastLsn:                    maxLSN.String(),
			}

			err = c.queries.UpdateCursor(ctx, tx, cursorParams)
			if err != nil {
				log.Printf("failed to update cursor: %v", err)
				return 0, err
			}
		}

		if err := tx.Commit(); err != nil {
			return 0, err
		}
	}

	return maxLSN, nil
}

func (c *Consumer) createFailedSet(failedIDs []int64) map[int64]bool {
	failedSet := make(map[int64]bool, len(failedIDs))
	for _, id := range failedIDs {
		failedSet[id] = true
	}

	return failedSet
}

func (c *Consumer) storeFailedMessages(ctx context.Context, tx *sql.Tx, ids []int64, payloads [][]byte) error {
	retryCounts := make([]int32, len(ids))
	errorMessages := make([]string, len(ids))
	stringPayloads := make([]string, len(payloads))

	for i, p := range payloads {
		stringPayloads[i] = string(p)
	}

	err := c.queries.InsertFailedBatch(ctx, tx, postgres.InsertFailedBatchParams{
		Ids:           ids,
		Payloads:      stringPayloads,
		ErrorMessages: errorMessages,
		RetryCounts:   retryCounts,
	})

	return err
}
