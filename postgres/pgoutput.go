package postgres

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// LSN is a Postgres log sequence number representing a position in the WAL.
type LSN = pglogrepl.LSN

// LogicalReplChange represents a database change captured via logical replication.
type LogicalReplChange struct {
	// ID is the job identifier from the database.
	ID int64
	// Payload is the job data as bytes (typically JSON).
	Payload []byte
	// CreatedAt is when the change was created in the database.
	CreatedAt time.Time
	// TransactionID is the transaction ID that produced this change.
	TransactionID int64
	// CommitLsn is the LSN as stored in the immediate table.
	CommitLsn string
	// LSN is the write-ahead log position of this change.
	LSN LSN
}

// RelationMetadata describes a database table for logical replication.
type RelationMetadata struct {
	// ID is the relation OID.
	ID uint32
	// Name is the table name.
	Name string
	// Columns describes the table columns.
	Columns []*pglogrepl.RelationMessageColumn
}

// MessageParser converts WAL messages into logical replication changes.
type MessageParser interface {
	// Parse decodes raw WAL data into a pglogrepl message.
	Parse(walData []byte) (pglogrepl.Message, error)
	// ParseRelation extracts table metadata from a relation message.
	ParseRelation(msg *pglogrepl.RelationMessage) *RelationMetadata
	// ParseInsert extracts change data from an insert message.
	ParseInsert(msg *pglogrepl.InsertMessageV2, relMeta *RelationMetadata, lsn LSN) (*LogicalReplChange, error)
}

// PgoutputParser implements MessageParser using the pgoutput plugin format.
type PgoutputParser struct{}

// NewPgoutputParser creates a new pgoutput message parser.
func NewPgoutputParser() *PgoutputParser {
	return &PgoutputParser{}
}

// Parse decodes raw WAL data into a pglogrepl message.
func (p *PgoutputParser) Parse(walData []byte) (pglogrepl.Message, error) {
	// I put inStream=false  for simplicity - basically I need to see what this does
	return pglogrepl.ParseV2(walData, false)
}

// ParseRelation extracts table metadata from a relation message.
func (p *PgoutputParser) ParseRelation(msg *pglogrepl.RelationMessage) *RelationMetadata {
	return &RelationMetadata{
		ID:      msg.RelationID,
		Name:    msg.RelationName,
		Columns: msg.Columns,
	}
}

// ParseInsert extracts change data from an insert message into a LogicalReplChange.
func (p *PgoutputParser) ParseInsert(msg *pglogrepl.InsertMessageV2, relMeta *RelationMetadata, lsn LSN) (*LogicalReplChange, error) {
	if msg.Tuple == nil {
		return nil, nil
	}

	change := &LogicalReplChange{
		LSN: lsn,
	}

	for i, col := range msg.Tuple.Columns {
		if i >= len(relMeta.Columns) {
			break
		}

		if col.DataType == pglogrepl.TupleDataTypeNull {
			continue
		}

		colName := relMeta.Columns[i].Name

		switch colName {
		case "id":
			id, err := col.Int64()
			if err != nil {
				return nil, fmt.Errorf("could not parse id: %w", err)
			}

			change.ID = id
		case "payload":
			change.Payload = col.Data
		case "created_at":
			var ts pgtype.Timestamptz
			err := ts.Scan(string(col.Data))
			if err != nil {
				return nil, errors.New("could not parse created_at timestamp: " + string(col.Data))
			}

			if ts.Valid {
				change.CreatedAt = ts.Time.UTC()
			}
		case "transaction_id":
			txID, err := col.Int64()
			if err != nil {
				return nil, fmt.Errorf("could not parse transaction_id: %w", err)
			}
			change.TransactionID = txID
		case "commit_lsn":
			change.CommitLsn = string(col.Data)
		}
	}

	if change.ID == 0 {
		return nil, errors.New("missing id column")
	}

	if len(change.Payload) == 0 {
		return nil, errors.New("missing payload column")
	}

	if change.CreatedAt.IsZero() {
		return nil, errors.New("missing created_at column")
	}

	return change, nil
}
