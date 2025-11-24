# Poutbox

A Go library that implements the outbox pattern using PostgreSQL. It supports two methods: polling and logical replication.

## Status

This package is in alpha state. It works, but features may change and edge cases need more testing. Consider it experimental for production use.

## The Problem

When building web services, you often need to handle background tasks reliably. For example:

1. User submits an order
2. Service saves the order in the database
3. Service sends a confirmation email in the background

Many teams use a separate message queue (RabbitMQ, Redis, Kafka, etc.) for background tasks.

### The Challenge

The difficulty is keeping the database and message queue in sync. Consider this code:

```go
tx := startTransaction()
order := createOrder(tx)
commit(tx)
publish_to_queue(order)  // What if this fails?
```

If `publish_to_queue` fails, the order is saved but the background task is lost.

You could wrap everything in a transaction, but then if the commit fails, you published a message for an order that was never saved.

Keeping a transaction open while calling external services also causes problems:
- Long transactions hold database locks
- This prevents maintenance operations like vacuum
- Locks can cause performance issues

### The Solution: Outbox Pattern

Instead of publishing directly, save a job record in the database as part of the same transaction:

```go
tx := startTransaction()
order := createOrder(tx)
save_to_outbox(tx, order_job)  // Same transaction
commit(tx)
```

Then a background process reads jobs from the outbox table and publishes them to your message queue. This ensures:

- Jobs are only created when the database transaction succeeds
- Jobs are never lost (they stay in the database until published)
- No long-running transactions blocking locks

## How Poutbox Works

Poutbox manages three tables:

1. **immediate** - stores jobs to be processed, partitioned by created_at
2. **scheduled** - stores jobs scheduled for future execution
3. **failed** - stores jobs that failed from immediate or scheduled tables
4. **dead_letter** - stores jobs that failed after max retries

The immediate table is partitioned by created_at to prevent unbounded growth. You can use the `Maintenance` struct to manage partitions automatically, or implement your own partition management. If you skip partitioning, you must manage deletions manually.

The library provides two ways to detect new jobs:

- **Polling** - efficiently tracks jobs using transaction IDs, prevents table bloat
- **Logical Replication** - uses PostgreSQL's WAL to detect new jobs

## Quick Start

### Prerequisites

- PostgreSQL 17 (possibly works on 13 also, need to test)
- Go 1.25 or newer

### Setup

1. Start PostgreSQL:

```bash
docker compose up
```

2. Create the tables (run once):

```bash
go run examples/simple/main.go -mode=migrate
```

### Using Polling

1. Start the consumer (reads jobs continously - sleeps every 100ms when no jobs found):

```bash
go run examples/simple/main.go -mode=consumer
```

2. In another terminal, create jobs:

```bash
go run examples/simple/main.go -mode=client
```

Press `CTRL+C` to stop each process.

Results are written to `jobs.txt`.

### Using Logical Replication

1. Start the consumer (real-time with logical replication):

```bash
go run examples/simple/main.go -mode=consumer-logical
```

2. In another terminal, create jobs:

```bash
go run examples/simple/main.go -mode=client
```

Press `CTRL+C` to stop.

Results are written to `jobs_logical_repl.txt`.

## Features

- **At-least-once delivery** - every job runs at least once
- **Job retries** - configurable max retries with exponential backoff
- **Scheduled jobs** - run jobs at a specific time
- **Dead letter queue** - failed jobs go here for manual review
- **Zero table bloat** - immediate jobs are removed without creating dead tuples
- **Two consumption modes** - polling or logical replication

## Performance

Consumer performance on a laptop:

- **Polling**: 13699 jobs/second
- **Logical Replication**: 52632 jobs/second  


## Architecture: Polling vs Logical Replication

### Polling

Polls the outbox table at regular intervals. Uses transaction IDs to avoid missing jobs and prevent table bloat.

**Note:** Long-running transactions can cause the poller to wait. Keep transaction times short.

### Logical Replication

Uses PostgreSQL's write-ahead log (WAL) to detect new jobs. No polling needed.

**Important:** If your application crashes, the replication slot stays active and consumes WAL space. Monitor slot size regularly.

## Usage

### Create a Consumer

```go
config := poutbox.ConsumerConfig{
    BatchSize:    100,
    MaxRetries:   3,
    PollInterval: 100 * time.Millisecond,
}

handler := MyJobHandler{} // implement poutbox.Handler interface
consumer := poutbox.NewConsumer(db, handler, config)

err := consumer.Start(ctx)
```

### Implement a Handler

```go
type MyHandler struct {}

func (h *MyHandler) Handle(ctx context.Context, jobs []poutbox.HandlerJob) []int64 {
    var failed []int64
    
    for _, job := range jobs {
        err := processJob(ctx, job.Payload)
        if err != nil {
            failed = append(failed, job.ID)
        }
    }
    
    return failed // return IDs of jobs to retry
}

func (h *MyHandler) Close(ctx context.Context) error {
    return nil
}
```

### Create a Client

```go
client := poutbox.NewClient(db)

// Enqueue a job for immediate processing
jobID, err := client.Enqueue(ctx, map[string]any{
    "order_id": 123,
    "email": "user@example.com",
})
```

### Enqueue with Transaction

Enqueue a job as part of your database transaction:

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()

// Create your business object
order, err := createOrder(ctx, tx, orderData)
if err != nil {
    return err
}

// Enqueue a background job in the same transaction
client := poutbox.NewClient(db)
_, err = client.Enqueue(ctx, map[string]any{
    "order_id": order.ID,
    "email": order.Email,
}, poutbox.WithTx(tx))
if err != nil {
    return err
}

// Both the order and the job are created only if commit succeeds
return tx.Commit()
```

### Schedule a Job for Later

```go
client := poutbox.NewClient(db)

// Schedule a job to run in 1 hour
jobID, err := client.Enqueue(ctx, map[string]any{
    "order_id": 123,
    "action": "send_reminder",
}, poutbox.WithScheduleAt(time.Now().Add(1*time.Hour)))

// Or schedule for a specific time
scheduledTime := time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC)
jobID, err = client.Enqueue(ctx, payload, poutbox.WithScheduleAt(scheduledTime))
```


## Development

To work on this library:

```bash
# Start the test database
docker compose up

# Run migrations
go run examples/simple/main.go -mode=migrate

# Run the example
go run examples/simple/main.go -mode=consumer
```

## License

See LICENSE file.
