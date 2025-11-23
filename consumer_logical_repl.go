package poutbox

import (
	"context"
	"errors"
	"log"
	"time"

	"poutbox/postgres"
)

const (
	logicalReplChannelBufferMultiplier   = 50
	logicalReplMinChannelBuffer          = 1000
	logicalReplMaxChannelBuffer          = 50000
	logicalReplBackpressureThreshold     = 0.8
	logicalReplBackpressureCheckInterval = 10 * time.Second
	logicalReplDefaultBatchTimeout       = 100 * time.Millisecond
	logicalReplReconnectionWaitTime      = 5 * time.Second
)

// processImmediateLogicalRepl starts the logical replication consumer with automatic reconnection
func (c *Consumer) processImmediateLogicalRepl(ctx context.Context) error {
	if err := postgres.InitializeLogicalReplication(ctx, c.db); err != nil {
		return err
	}

	if c.replConnStr == "" {
		return errors.New("replication connection string not set")
	}

	batchSize := c.validateBatchSize(c.config.LogicalReplBatchSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := c.runLogicalReplicationSession(ctx, batchSize); err != nil {
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

func (c *Consumer) validateBatchSize(configSize int) int {
	if configSize <= 0 {
		return 100
	}

	return configSize
}

func (c *Consumer) runLogicalReplicationSession(ctx context.Context, batchSize int) error {
	log.Printf("Starting logical replication session with batch size %d", batchSize)

	stream, err := postgres.NewReplicationStream(ctx, c.replConnStr)
	if err != nil {
		log.Printf("Failed to create replication stream: %v", err)
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*3)
		defer cancel()

		_ = stream.Close(closeCtx)
	}()

	log.Printf("Replication stream connected, starting message reading")
	changesChan := c.streamReader(ctx, stream, batchSize)
	errChan := c.changeBatcher(ctx, stream, changesChan, batchSize)

	for err := range errChan {
		if err != nil {
			log.Printf("Replication session error: %v", err)

			return err
		}
	}

	return nil
}

func (c *Consumer) streamReader(ctx context.Context, stream *postgres.ReplicationStream, batchSize int) <-chan postgres.LogicalReplChange {
	bufferSize := c.calculateChannelBufferSize(batchSize)
	changesChan := make(chan postgres.LogicalReplChange, bufferSize)

	log.Printf("Stream reader starting with buffer size %d", bufferSize)

	go c.monitorBackpressure(ctx, changesChan, bufferSize)

	go func() {
		defer close(changesChan)

		log.Printf("Starting to read replication stream...")

		c.readReplicationStream(ctx, stream, changesChan)

		log.Printf("Replication stream reader closed")
	}()

	return changesChan
}

func (c *Consumer) calculateChannelBufferSize(batchSize int) int {
	bufferSize := max(batchSize*logicalReplChannelBufferMultiplier, logicalReplMinChannelBuffer)
	bufferSize = min(bufferSize, logicalReplMaxChannelBuffer)

	return bufferSize
}

func (c *Consumer) monitorBackpressure(ctx context.Context, changesChan <-chan postgres.LogicalReplChange, bufferSize int) {
	ticker := time.NewTicker(logicalReplBackpressureCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if len(changesChan) > int(float64(bufferSize)*logicalReplBackpressureThreshold) {
				utilization := float64(len(changesChan)) / float64(bufferSize) * 100

				log.Printf("Backpressure warning: channel at %d/%d capacity (%.1f%%)", len(changesChan), bufferSize, utilization)
			}
		}
	}
}

func (c *Consumer) readReplicationStream(ctx context.Context, stream *postgres.ReplicationStream, changesChan chan<- postgres.LogicalReplChange) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := stream.ReceiveMessage(ctx, 30*c.config.PollInterval) // is timeout ok here?
		if err != nil {
			log.Printf("Error receiving replication message: %v", err)

			return
		}

		if msg == nil || msg.Type == "timeout" {
			continue
		}

		if msg.Type == "commit" && len(msg.Changes) > 0 {
			c.sendChangesToChannel(ctx, changesChan, msg.Changes)

			continue
		}

		if msg.Type == "abort" {
			log.Printf("Skipping aborted transaction")

			continue
		}
	}
}

func (c *Consumer) sendChangesToChannel(ctx context.Context, changesChan chan<- postgres.LogicalReplChange, changes []postgres.LogicalReplChange) {
	for _, change := range changes {
		select {
		case changesChan <- change:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) changeBatcher(ctx context.Context, stream *postgres.ReplicationStream, changesChan <-chan postgres.LogicalReplChange, batchSize int) <-chan error {
	errChan := make(chan error, 2)

	go func() {
		defer close(errChan)

		c.batchChanges(ctx, stream, changesChan, batchSize, errChan)
	}()

	return errChan
}

func (c *Consumer) batchChanges(ctx context.Context, stream *postgres.ReplicationStream, changesChan <-chan postgres.LogicalReplChange, batchSize int, errChan chan<- error) {
	var batch []postgres.LogicalReplChange
	ticker := time.NewTicker(logicalReplDefaultBatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				log.Printf("Context canceled with %d changes in batch, processing final batch", len(batch))

				noCancel := context.WithoutCancel(ctx)
				finalCtx, cancel := context.WithTimeout(noCancel, 10*time.Second)

				if err := c.processBatch(finalCtx, stream, batch); err != nil {
					log.Printf("ERROR: Failed to process final batch: %v", err)
				}

				cancel()
			}

			noCancel := context.WithoutCancel(ctx)
			ackCtx, cancel := context.WithTimeout(noCancel, 10*time.Second)
			if err := c.acknowledgeProcessing(ackCtx, stream); err != nil {
				log.Printf("ERROR: Failed to acknowledge final batch: %v", err)
			}
			cancel()

			select {
			case errChan <- ctx.Err():
			default:
			}

			return
		case change, ok := <-changesChan:
			if !ok {
				return
			}

			batch = append(batch, change)

			if len(batch) >= batchSize {
				if err := c.processBatchAndAck(ctx, stream, batch); err != nil {
					select {
					case errChan <- err:
					case <-ctx.Done():
						return
					}

					return
				}

				batch = nil
				ticker.Reset(logicalReplDefaultBatchTimeout)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				if err := c.processBatchAndAck(ctx, stream, batch); err != nil {
					select {
					case errChan <- err:
					case <-ctx.Done():
						return
					}

					return
				}

				batch = nil
			}
		}
	}
}

func (c *Consumer) processBatch(ctx context.Context, _ *postgres.ReplicationStream, batch []postgres.LogicalReplChange) error {
	if len(batch) == 0 {
		return nil
	}

	handlerJobs := make([]HandlerJob, len(batch))
	for i, ch := range batch {
		handlerJobs[i] = HandlerJob{ID: ch.ID, Payload: ch.Payload}
	}

	failedIDs := c.handler.Handle(ctx, handlerJobs)
	failedSet := c.createFailedSet(failedIDs)

	maxLSN := uint64(0)

	var (
		toRetry         []int64
		toRetryPayloads [][]byte
	)

	for _, ch := range batch {
		if uint64(ch.LSN) > maxLSN {
			maxLSN = uint64(ch.LSN)
		}

		if failedSet[ch.ID] {
			toRetry = append(toRetry, ch.ID)
			toRetryPayloads = append(toRetryPayloads, ch.Payload)
		}
	}

	if len(toRetry) > 0 {
		if err := c.storeFailedMessages(ctx, toRetry, toRetryPayloads); err != nil {
			return err
		}
	}

	if maxLSN > 0 {
		c.lastProcessedLSN = postgres.LSN(maxLSN)
	}

	return nil
}

func (c *Consumer) acknowledgeProcessing(ctx context.Context, stream *postgres.ReplicationStream) error {
	if err := stream.SendKeepalive(ctx); err != nil {
		return err
	}

	if c.lastProcessedLSN > 0 {
		stream.UpdateProcessedLSN(c.lastProcessedLSN)
		c.lastProcessedLSN = 0
	}

	return nil
}

func (c *Consumer) processBatchAndAck(ctx context.Context, stream *postgres.ReplicationStream, batch []postgres.LogicalReplChange) error {
	if err := c.processBatch(ctx, stream, batch); err != nil {
		return err
	}

	if err := c.acknowledgeProcessing(ctx, stream); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) createFailedSet(failedIDs []int64) map[int64]bool {
	failedSet := make(map[int64]bool, len(failedIDs))
	for _, id := range failedIDs {
		failedSet[id] = true
	}

	return failedSet
}

func (c *Consumer) storeFailedMessages(ctx context.Context, ids []int64, payloads [][]byte) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	retryCounts := make([]int32, len(ids))
	for i := range retryCounts {
		retryCounts[i] = 0
	}

	errorMessages := make([]string, len(ids))

	err = c.queries.InsertFailedBatch(ctx, tx, postgres.InsertFailedBatchParams{
		Ids:           ids,
		Payloads:      payloads,
		ErrorMessages: errorMessages,
		RetryCounts:   retryCounts,
	})
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
