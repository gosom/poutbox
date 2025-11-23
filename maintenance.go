package poutbox

import (
	"context"
	"database/sql"
	"log"
	"time"

	"poutbox/postgres"
)

type Maintenance struct {
	partitionOps *postgres.PartitionOps

	partitionInterval time.Duration
	lookAhead         time.Duration
	retentionWindow   time.Duration
}

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

type MaintenanceOption func(*Maintenance)

func WithPartitionInterval(d time.Duration) MaintenanceOption {
	return func(m *Maintenance) {
		m.partitionInterval = d
	}
}

func WithLookAhead(d time.Duration) MaintenanceOption {
	return func(m *Maintenance) {
		m.lookAhead = d
	}
}

func WithRetentionWindow(d time.Duration) MaintenanceOption {
	return func(m *Maintenance) {
		m.retentionWindow = d
	}
}

func (m *Maintenance) Run(ctx context.Context) error {
	now := time.Now().UTC()
	fromBoundary := m.currentBoundary(now).Add(-m.partitionInterval)
	endTime := now.Add(m.lookAhead)
	cutoffTime := now.Add(-m.retentionWindow)

	return m.partitionOps.Run(ctx, fromBoundary, endTime, m.partitionInterval, cutoffTime)
}

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
