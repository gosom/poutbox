package postgres

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	defaultConnectionRetries    = 5
	defaultConnectionRetryDelay = 2 * time.Second
)

type ReplicationStream struct {
	conn         *pgconn.PgConn
	parser       MessageParser
	relations    map[uint32]*RelationMetadata
	currentTx    *TransactionBuffer
	serverWALEnd LSN
	processedLSN LSN
}

type TransactionBuffer struct {
	Changes []LogicalReplChange
}

type ReplicationMessage struct {
	// Type is commit, abort or timeout
	Type    string
	Changes []LogicalReplChange
	IsAbort bool
}

func NewReplicationStream(ctx context.Context, connStr string) (*ReplicationStream, error) {
	return NewReplicationStreamWithRetry(ctx, connStr, defaultConnectionRetries, defaultConnectionRetryDelay)
}

func NewReplicationStreamWithRetry(ctx context.Context, connStr string, maxRetries int, retryDelay time.Duration) (*ReplicationStream, error) {
	var lastErr error

	for attempt := range maxRetries {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(retryDelay):
				log.Printf("Retrying replication connection (attempt %d/%d)", attempt+1, maxRetries)
			}
		}

		conn, err := pgconn.Connect(ctx, connStr)
		if err != nil {
			lastErr = err
			log.Printf("Failed to connect to replication server: %v", err)

			continue
		}

		if _, err := pglogrepl.IdentifySystem(ctx, conn); err != nil {
			_ = conn.Close(ctx)

			lastErr = err
			log.Printf("Failed to identify replication system: %v", err)

			continue
		}

		if err := pglogrepl.StartReplication(ctx, conn, ReplicationSlot, 0, pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '2'",
				"publication_names '" + PublicationName + "'",
				"messages 'off'",
			},
		}); err != nil {
			_ = conn.Close(ctx)

			lastErr = err
			log.Printf("Failed to start replication: %v", err)

			continue
		}

		log.Printf("Successfully connected to replication stream")

		return &ReplicationStream{
			conn:      conn,
			parser:    NewPgoutputParser(),
			relations: make(map[uint32]*RelationMetadata),
		}, nil
	}

	return nil, lastErr
}

func (rs *ReplicationStream) processWALMessage(pgMsg pglogrepl.Message) (bool, *ReplicationMessage) {
	switch pgMsg := pgMsg.(type) {
	case *pglogrepl.RelationMessage:
		// V1 relation message, for a reason I get V1 and V2 but proto version is 2. For now handle both.
		if isImmediateTable(pgMsg.RelationName) {
			rs.relations[pgMsg.RelationID] = rs.parser.ParseRelation(pgMsg)
		}

		return false, nil

	case *pglogrepl.RelationMessageV2:
		if isImmediateTable(pgMsg.RelationName) {
			rel := &pglogrepl.RelationMessage{
				RelationID:      pgMsg.RelationID,
				Namespace:       pgMsg.Namespace,
				RelationName:    pgMsg.RelationName,
				ReplicaIdentity: pgMsg.ReplicaIdentity,
				Columns:         pgMsg.Columns,
			}
			rs.relations[pgMsg.RelationID] = rs.parser.ParseRelation(rel)
		}

		return false, nil

	case *pglogrepl.BeginMessage:
		rs.currentTx = &TransactionBuffer{Changes: []LogicalReplChange{}}

		return false, nil

	case *pglogrepl.StreamStartMessageV2:
		// this is for streaming transactions - i treat as begin - but stream is false
		// this needs to be checked
		rs.currentTx = &TransactionBuffer{Changes: []LogicalReplChange{}}

		return false, nil

	case *pglogrepl.CommitMessage:
		if rs.currentTx != nil && len(rs.currentTx.Changes) > 0 {
			msg := &ReplicationMessage{
				Type:    "commit",
				Changes: rs.currentTx.Changes,
			}
			rs.currentTx = nil

			return true, msg
		}

		rs.currentTx = nil

		return false, nil

	case *pglogrepl.StreamCommitMessageV2:
		// similarly like commit - but I haven't checked this one
		if rs.currentTx != nil && len(rs.currentTx.Changes) > 0 {
			msg := &ReplicationMessage{
				Type:    "commit",
				Changes: rs.currentTx.Changes,
			}
			rs.currentTx = nil

			return true, msg
		}

		rs.currentTx = nil

		return false, nil

	case *pglogrepl.InsertMessageV2:
		relMeta, ok := rs.relations[pgMsg.RelationID]
		if !ok || !isImmediateTable(relMeta.Name) {
			return false, nil
		}

		change, err := rs.parser.ParseInsert(pgMsg, relMeta, rs.serverWALEnd)
		if err != nil {
			log.Printf("Failed to extract change: %v", err)

			return false, nil
		}

		if change != nil {
			if rs.currentTx == nil {
				rs.currentTx = &TransactionBuffer{}
			}

			rs.currentTx.Changes = append(rs.currentTx.Changes, *change)
		}

		return false, nil

	case *pglogrepl.StreamAbortMessageV2:
		if rs.currentTx != nil {
			msg := &ReplicationMessage{
				Type:    "abort",
				IsAbort: true,
			}
			rs.currentTx = nil
			return true, msg
		}

		rs.currentTx = nil

		return false, nil

	// We ignore these weonly need inserts for immediate tables
	// but added here since I added for debug/dev purposes
	case *pglogrepl.UpdateMessageV2:
		return false, nil

	case *pglogrepl.DeleteMessageV2:
		return false, nil

	case *pglogrepl.TruncateMessageV2:
		return false, nil

	case *pglogrepl.TypeMessageV2:
		return false, nil

	case *pglogrepl.OriginMessage:
		return false, nil

	default:
		return false, nil
	}
}

func (rs *ReplicationStream) ReceiveMessage(ctx context.Context, timeout time.Duration) (*ReplicationMessage, error) {
	if rs.conn.IsClosed() {
		return nil, errors.New("replication connection is closed")
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	msg, err := rs.conn.ReceiveMessage(timeoutCtx)
	cancel()

	if err != nil {
		if pgconn.Timeout(err) {
			return &ReplicationMessage{Type: "timeout"}, nil
		}

		return nil, err
	}

	copyData, ok := msg.(*pgproto3.CopyData)
	if !ok {
		return nil, nil
	}

	if len(copyData.Data) < 1 {
		return nil, errors.New("empty CopyData message")
	}

	// see the example in pglogrepl
	switch copyData.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
		if err != nil {
			return nil, err
		}

		rs.serverWALEnd = pkm.ServerWALEnd

		if pkm.ReplyRequested {
			if err := rs.SendKeepalive(ctx); err != nil {
				return nil, err
			}
		}

		return &ReplicationMessage{Type: "timeout"}, nil
	case pglogrepl.XLogDataByteID:
		// see the pglogrepl example
		xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		if err != nil {
			return nil, err
		}

		rs.serverWALEnd = xld.ServerWALEnd

		if len(xld.WALData) > 0 {
			pgMsg := rs.parseWALMessageSafely(xld.WALData)
			if pgMsg == nil {
				return &ReplicationMessage{Type: "timeout"}, nil
			}

			shouldReturn, replicationMsg := rs.processWALMessage(pgMsg)
			if shouldReturn {
				return replicationMsg, nil
			}
		}

	default:
		log.Printf("Unknown CopyData message type: %c (0x%02x)", copyData.Data[0], copyData.Data[0])
		return &ReplicationMessage{Type: "timeout"}, nil
	}

	return nil, nil
}

func (rs *ReplicationStream) SendKeepalive(ctx context.Context) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, rs.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: rs.serverWALEnd,
		WALFlushPosition: rs.serverWALEnd,
		WALApplyPosition: rs.processedLSN,
		ClientTime:       time.Now(),
	})
}

func (rs *ReplicationStream) GetPendingChanges() []LogicalReplChange {
	if rs.currentTx == nil {
		return nil
	}
	return rs.currentTx.Changes
}

func (rs *ReplicationStream) ClearPending() {
	rs.currentTx = nil
}

func (rs *ReplicationStream) UpdateProcessedLSN(lsn LSN) {
	if lsn > rs.processedLSN {
		rs.processedLSN = lsn
	}
}

func (rs *ReplicationStream) Close(ctx context.Context) error {
	if rs.conn != nil {
		_ = rs.conn.Close(ctx)
	}

	return nil
}

func (rs *ReplicationStream) IsTimeout(err error) bool {
	return pgconn.Timeout(err)
}

func isImmediateTable(tableName string) bool {
	return tableName == "immediate" || strings.HasPrefix(tableName, "immediate_")
}

func (rs *ReplicationStream) parseWALMessageSafely(walData []byte) pglogrepl.Message {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Warning: WAL message parsing panicked: %v (will skip)", r)
		}
	}()

	msg, err := rs.parser.Parse(walData)
	if err != nil {
		log.Printf("Warning: could not parse WAL message: %v (will skip)", err)
		return nil
	}

	return msg
}
