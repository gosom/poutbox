-- name: EnqueueImmediate :one
INSERT INTO "poutbox".immediate (payload)
VALUES ($1)
RETURNING id;

-- name: EnqueueScheduled :one
INSERT INTO "poutbox".scheduled (payload, scheduled_at)
VALUES ($1, $2)
RETURNING id;

-- name: InsertPartitionMeta :exec
INSERT INTO "poutbox".partition_meta (partition_name, range_start, range_end)
VALUES ($1, $2, $3)
ON CONFLICT (partition_name) DO NOTHING;

-- name: DeletePartitionMeta :exec
DELETE FROM "poutbox".partition_meta
WHERE partition_name = $1;

-- name: ListPartitions :many
SELECT partition_name, range_start, range_end
FROM "poutbox".partition_meta
ORDER BY range_start ASC;

-- name: GetFailedJobsReady :many
SELECT id, payload, error_message, retry_count, failed_at, next_retry_at
FROM "poutbox".failed
WHERE next_retry_at <= NOW() AT TIME ZONE 'UTC'
ORDER BY next_retry_at, id
LIMIT @batch_size;

-- name: GetImmediateJobs :many
SELECT id, payload, created_at, transaction_id
FROM "poutbox".immediate
WHERE
  created_at >= @last_processed_at::timestamptz
  AND (
    (transaction_id = @last_processed_transaction_id::bigint AND id > @last_processed_id::bigint)
    OR (transaction_id > @last_processed_transaction_id::bigint)
  )
  AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())::text::bigint
ORDER BY transaction_id ASC, id ASC
LIMIT @batch_size;

-- name: GetCursor :one
SELECT id, last_processed_id, last_processed_at, last_processed_transaction_id, updated_at
FROM "poutbox".cursor
WHERE id = 1;

-- name: UpdateCursor :exec
UPDATE "poutbox".cursor
SET last_processed_id = @last_processed_id::bigint, last_processed_at = @last_processed_at::timestamptz, last_processed_transaction_id = @last_processed_transaction_id::bigint, updated_at = NOW() AT TIME ZONE 'UTC'
WHERE id = 1;

-- name: DeleteFailed :exec
DELETE FROM "poutbox".failed
WHERE id = @id::bigint;

-- name: InsertFailed :exec
INSERT INTO "poutbox".failed (id, payload, error_message, retry_count, failed_at, next_retry_at)
VALUES (@id::bigint, @payload, @error_message, @retry_count, NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC')
ON CONFLICT (id) DO UPDATE SET
    error_message = @error_message,
    retry_count = @retry_count,
    next_retry_at = NOW() AT TIME ZONE 'UTC';

-- name: UpdateFailed :exec
UPDATE "poutbox".failed
SET error_message = @error_message, retry_count = @retry_count, next_retry_at = NOW() AT TIME ZONE 'UTC'
WHERE id = @id::bigint;

-- name: InsertDeadLetter :exec
INSERT INTO "poutbox".dead_letter (id, payload, error_message, retry_count, failed_at)
VALUES (@id::bigint, @payload, @error_message, @retry_count, NOW() AT TIME ZONE 'UTC');

-- name: DeleteImmediate :exec
DELETE FROM "poutbox".immediate
WHERE id = @id::bigint;

-- name: DeleteFailedBatch :exec
DELETE FROM "poutbox".failed
WHERE id = ANY(@ids::bigint[]);

-- name: InsertFailedBatch :exec
INSERT INTO "poutbox".failed (id, payload, error_message, retry_count, failed_at, next_retry_at)
SELECT t.id, t.payload, t.error_message, t.retry_count, NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC'
FROM (
  SELECT
    UNNEST(@ids::bigint[]) as id,
    UNNEST(@payloads::text[]) as payload,
    UNNEST(@error_messages::text[]) as error_message,
    UNNEST(@retry_counts::integer[]) as retry_count
) t
ON CONFLICT (id) DO UPDATE SET
    error_message = EXCLUDED.error_message,
    retry_count = EXCLUDED.retry_count,
    next_retry_at = NOW() AT TIME ZONE 'UTC';

-- name: InsertDeadLetterBatch :exec
INSERT INTO "poutbox".dead_letter (id, payload, error_message, retry_count, failed_at)
SELECT t.id, t.payload, t.error_message, t.retry_count, NOW() AT TIME ZONE 'UTC'
FROM (
  SELECT
    UNNEST(@ids::bigint[]) as id,
    UNNEST(@payloads::text[]) as payload,
    UNNEST(@error_messages::text[]) as error_message,
    UNNEST(@retry_counts::integer[]) as retry_count
) t;

-- name: GetScheduledJobsReady :many
SELECT id, payload, scheduled_at
FROM "poutbox".scheduled
WHERE scheduled_at <= NOW() AT TIME ZONE 'UTC'
ORDER BY scheduled_at, id
LIMIT @batch_size;

-- name: DeleteScheduledBatch :exec
DELETE FROM "poutbox".scheduled
WHERE id = ANY(@ids::bigint[]);

