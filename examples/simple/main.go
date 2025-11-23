package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"maps"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"poutbox"

	"poutbox/postgres"
)

func main() {
	allowedModes := []string{"client", "consumer", "consumer-logical", "maintenance", "migrate"}
	var mode string

	flag.StringVar(&mode, "mode", "", "Mode to run: "+strings.Join(allowedModes, ", "))
	flag.Parse()

	if !slices.Contains(allowedModes, mode) {
		flag.Usage()
		os.Exit(2)
	}

	if err := run(mode); err != nil {
		log.Printf("fatal error: %v\n", err)
		os.Exit(1)
	}
}

func run(mode string) error {
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://poutbox:poutbox@localhost:5432/poutbox?sslmode=disable"
	}

	db, err := postgres.Connect(ctx, connStr)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	if err = postgres.Migrate(ctx, db); err != nil {
		return err
	}

	pm := poutbox.NewMaintenance(db)
	if err = pm.Run(ctx); err != nil {
		return err
	}

	if mode == "migrate" {
		return nil
	}

	switch mode {
	case "maintenance":
		return nil
	case "client":
		return runClient(ctx, db)
	case "consumer":
		return runConsumer(ctx, db)
	case "consumer-logical":
		return runConsumerLogicalRepl(ctx, db, connStr)
	default:
		return errors.New("unsupported mode: " + mode)
	}
}

func runClient(ctx context.Context, db *sql.DB) error {
	client := poutbox.NewClient(db)

	numWorkers := runtime.NumCPU()
	var cnt atomic.Int64
	var wg sync.WaitGroup

	for range numWorkers {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				n := cnt.Add(1)
				_, err := client.Enqueue(ctx, map[string]any{
					"message": "Hello poutbox",
					"count":   n,
				})
				if err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("failed to enqueue job: %v", err)
				}
			}
		})
	}

	wg.Wait()

	return nil
}

type fileJobHandler struct {
	fd *os.File
}

func (h *fileJobHandler) Handle(ctx context.Context, jobs []poutbox.HandlerJob) []int64 {
	failed := make(map[int64]struct{})
	sb := strings.Builder{}

	for _, job := range jobs {
		var payload map[string]any
		err := json.Unmarshal(job.Payload, &payload)
		if err != nil {
			failed[job.ID] = struct{}{}
			continue
		}

		_, err = sb.WriteString(fmt.Sprintf("%v\n", job.ID))
		if err != nil {
			failed[job.ID] = struct{}{}
		}
	}

	_, err := h.fd.WriteString(sb.String())
	if err != nil {
		log.Printf("failed to write to file: %v", err)
		for _, job := range jobs {
			if _, ok := failed[job.ID]; !ok {
				failed[job.ID] = struct{}{}
			}
		}
	}

	return slices.Collect(maps.Keys(failed))
}

func (h *fileJobHandler) Close(context.Context) error {
	if h.fd == nil {
		return nil
	}

	return h.fd.Close()
}

func createJobHandler(filename string) poutbox.Handler {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		panic(err)
	}
	ans := fileJobHandler{
		fd: fd,
	}

	return &ans
}

func runConsumer(ctx context.Context, db *sql.DB) error {
	handler := createJobHandler("jobs.txt")

	config := poutbox.ConsumerConfig{
		BatchSize:    1000,
		MaxRetries:   3,
		PollInterval: 100 * time.Millisecond,
	}
	consumer := poutbox.NewConsumer(db, handler, config)

	return consumer.Start(ctx)
}

func runConsumerLogicalRepl(ctx context.Context, db *sql.DB, connStr string) error {
	handler := createJobHandler("jobs_logical_repl.txt")

	config := poutbox.ConsumerConfig{
		BatchSize:             1000,
		MaxRetries:            3,
		PollInterval:          100 * time.Millisecond,
		UseLogicalReplication: true,
		LogicalReplBatchSize:  1000,
	}

	replConnStr := connStr
	if !strings.Contains(connStr, "replication=") {
		if strings.Contains(connStr, "?") {
			replConnStr = connStr + "&replication=database"
		} else {
			replConnStr = connStr + "?replication=database"
		}
	}

	consumer := poutbox.NewConsumer(db, handler, config)
	consumer.SetReplicationConnString(replConnStr)
	defer func() {
		_ = consumer.Close(context.WithoutCancel(ctx))
	}()

	log.Println("Starting consumer with logical replication...")

	return consumer.Start(ctx)
}
