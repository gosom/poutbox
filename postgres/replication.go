package postgres

import (
	"context"
	"fmt"
	"iter"
	"log"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	ReplicationEventTypeKeepalive = "keepalive"
	ReplicationEventTypeInsert    = "insert"
)

type ReplicationEvent struct {
	Type      string
	Change    *LogicalReplChange
	Keepalive *KeepaliveRequest
}

type KeepaliveRequest struct {
	ServerWALEnd   LSN
	ReplyRequested bool
}

type ReplicationStream struct {
	conn         *pgconn.PgConn
	parser       MessageParser
	relations    map[uint32]*RelationMetadata
	serverWALEnd LSN
}

func NewReplicationStream(ctx context.Context, connStr string, startLSN LSN) (*ReplicationStream, error) {
	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}

	if _, err := pglogrepl.IdentifySystem(ctx, conn); err != nil {
		conn.Close(ctx)
		return nil, err
	}

	if err := pglogrepl.StartReplication(ctx, conn, ReplicationSlot, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '2'",
			"publication_names '" + PublicationName + "'",
			"messages 'off'",
		},
	}); err != nil {
		conn.Close(ctx)
		return nil, err
	}

	return &ReplicationStream{
		conn:      conn,
		parser:    NewPgoutputParser(),
		relations: make(map[uint32]*RelationMetadata),
	}, nil
}

func (rs *ReplicationStream) Events(ctx context.Context) iter.Seq2[ReplicationEvent, error] {
	return func(yield func(ReplicationEvent, error) bool) {
		var buffer []*LogicalReplChange

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msg, err := rs.conn.ReceiveMessage(ctx)
			if err != nil {
				if !yield(ReplicationEvent{}, err) {
					return
				}

				continue
			}

			copyData, ok := msg.(*pgproto3.CopyData)
			if !ok || len(copyData.Data) < 1 {
				continue
			}

			switch copyData.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					if !yield(ReplicationEvent{}, err) {
						return
					}
					continue
				}

				rs.serverWALEnd = pkm.ServerWALEnd
				event := ReplicationEvent{
					Type: ReplicationEventTypeKeepalive,
					Keepalive: &KeepaliveRequest{
						ServerWALEnd:   pkm.ServerWALEnd,
						ReplyRequested: pkm.ReplyRequested,
					},
				}
				if !yield(event, nil) {
					return
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					if !yield(ReplicationEvent{}, err) {
						return
					}
					continue
				}
				rs.serverWALEnd = xld.ServerWALEnd

				if len(xld.WALData) > 0 {
					pgMsg, err := rs.parseWALMessageSafely(xld.WALData)
					if err != nil {
						if !yield(ReplicationEvent{}, err) {
							return
						}
						continue
					}

					switch m := pgMsg.(type) {
					case *pglogrepl.RelationMessage:
						if isImmediateTable(m.RelationName) {
							rs.relations[m.RelationID] = rs.parser.ParseRelation(m)
						}
					case *pglogrepl.RelationMessageV2:
						if isImmediateTable(m.RelationName) {
							rel := &pglogrepl.RelationMessage{
								RelationID:      m.RelationID,
								Namespace:       m.Namespace,
								RelationName:    m.RelationName,
								ReplicaIdentity: m.ReplicaIdentity,
								Columns:         m.Columns,
							}

							rs.relations[m.RelationID] = rs.parser.ParseRelation(rel)
						}
					case *pglogrepl.InsertMessageV2:
						rel, ok := rs.relations[m.RelationID]
						if !ok || !isImmediateTable(rel.Name) {
							continue
						}

						change, err := rs.parser.ParseInsert(m, rel, rs.serverWALEnd)
						if err != nil {
							if !yield(ReplicationEvent{}, err) {
								return
							}

							continue
						}

						if change != nil {
							buffer = append(buffer, change)
						}
					case *pglogrepl.StreamAbortMessageV2:
						buffer = nil
					case *pglogrepl.CommitMessage:
						for _, change := range buffer {
							event := ReplicationEvent{
								Type:   ReplicationEventTypeInsert,
								Change: change,
							}
							if !yield(event, nil) {
								return
							}
						}

						buffer = buffer[:0]
					}
				}
			}
		}
	}
}

func (rs *ReplicationStream) SendKeepalive(ctx context.Context, walApplyPosition LSN) error {
	// when we restart the application, the context will be cancelled.
	// this will cause not sending the status update, so the already procesed WAL
	// will be re-sent by PostgreSQL when the application restarts.
	// This will cause duplicate processing of some messages
	// so we use a context without cancellation here and it's own timeout.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*3)
	defer cancel()
	return pglogrepl.SendStandbyStatusUpdate(ctx, rs.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: rs.serverWALEnd,
		WALFlushPosition: rs.serverWALEnd,
		WALApplyPosition: walApplyPosition,
		ClientTime:       time.Now().UTC(),
	})
}

func (rs *ReplicationStream) Close(ctx context.Context) error {
	if rs.conn != nil {
		_ = rs.conn.Close(ctx)
	}
	return nil
}

func isImmediateTable(tableName string) bool {
	return tableName == "immediate" || strings.HasPrefix(tableName, "immediate_")
}

func (rs *ReplicationStream) parseWALMessageSafely(walData []byte) (pglogrepl.Message, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Warning: WAL message parsing panicked: %v", r)
			err = fmt.Errorf("WAL parsing panic: %v", r)
		}
	}()

	msg, err := rs.parser.Parse(walData)
	if err != nil {
		log.Printf("Warning: could not parse WAL message: %v", err)
		return nil, err
	}

	return msg, nil
}
