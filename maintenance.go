package poutbox

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/gosom/poutbox/postgres"
)

// Maintenance manages database partitions and cleanup.
// It creates future partitions and removes old data based on retention settings.
type Maintenance struct {
	// partitionOps executes partition operations.
	partitionOps *postgres.PartitionOps
	// partitionInterval is the time span of each partition.
	partitionInterval time.Duration
	// lookAhead is how far in the future to pre-create partitions.
	lookAhead time.Duration
	// retentionWindow is how long to keep old data before deletion.
	retentionWindow time.Duration
}

// NewMaintenance creates a new Maintenance instance with the given database.
// Sets defaults: PartitionInterval (1h), LookAhead (12h), RetentionWindow (3h).
func NewMaintenance(db *sql.DB, opts ...MaintenanceOption) *Maintenance {
	m := &Maintenance{
		partitionOps:      postgres.NewPartitionOps(db),
		partitionInterval: time.Hour,
		lookAhead:         12 * time.Hour,
		retentionWindow:   3 * time.Hour,
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// MaintenanceOption is a function that configures maintenance behavior.
type MaintenanceOption func(*Maintenance)

// WithPartitionInterval sets the time span of each partition.
func WithPartitionInterval(d time.Duration) MaintenanceOption {
	return func(m *Maintenance) {
		m.partitionInterval = d
	}
}

// WithLookAhead sets how far in the future to pre-create partitions.
func WithLookAhead(d time.Duration) MaintenanceOption {
	return func(m *Maintenance) {
		m.lookAhead = d
	}
}

// WithRetentionWindow sets how long to keep old data before deletion.
func WithRetentionWindow(d time.Duration) MaintenanceOption {
	return func(m *Maintenance) {
		m.retentionWindow = d
	}
}

// Run executes a single maintenance cycle: creates future partitions and removes old data.
func (m *Maintenance) Run(ctx context.Context) error {
	now := time.Now().UTC()
	fromBoundary := m.currentBoundary(now).Add(-m.partitionInterval)
	endTime := now.Add(m.lookAhead)
	cutoffTime := now.Add(-m.retentionWindow)

	return m.partitionOps.Run(ctx, fromBoundary, endTime, m.partitionInterval, cutoffTime)
}

// RunPeriodically executes maintenance at regular intervals until context is canceled.
// Runs immediately on start, then at each interval. Logs errors without stopping.
func (m *Maintenance) RunPeriodically(ctx context.Context, interval time.Duration) {
	if err := m.Run(ctx); err != nil {
		log.Printf("maintenance error: %v", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.Run(ctx); err != nil {
				log.Printf("maintenance error: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *Maintenance) currentBoundary(t time.Time) time.Time {
	return t.Truncate(m.partitionInterval)
}
