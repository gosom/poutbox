package postgres

import (
	"errors"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

type LSN = pglogrepl.LSN

type LogicalReplChange struct {
	ID        int64
	Payload   []byte
	CreatedAt time.Time
	LSN       LSN
}

type RelationMetadata struct {
	ID      uint32
	Name    string
	Columns []*pglogrepl.RelationMessageColumn
}

type MessageParser interface {
	Parse(walData []byte) (pglogrepl.Message, error)
	ParseRelation(msg *pglogrepl.RelationMessage) *RelationMetadata
	ParseInsert(msg *pglogrepl.InsertMessageV2, relMeta *RelationMetadata, lsn LSN) (*LogicalReplChange, error)
}

type PgoutputParser struct{}

func NewPgoutputParser() *PgoutputParser {
	return &PgoutputParser{}
}

func (p *PgoutputParser) Parse(walData []byte) (pglogrepl.Message, error) {
	// I put inStream=false  for simplicity - basically I need to see what this does
	return pglogrepl.ParseV2(walData, false)
}

func (p *PgoutputParser) ParseRelation(msg *pglogrepl.RelationMessage) *RelationMetadata {
	return &RelationMetadata{
		ID:      msg.RelationID,
		Name:    msg.RelationName,
		Columns: msg.Columns,
	}
}

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
				return nil, err
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
