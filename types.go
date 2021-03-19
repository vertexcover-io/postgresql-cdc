package cdc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
)

// taken from https://github.com/chobostar/pg_listener

const MsgTimestampFormat = "2006-01-02 15:04:05.999999-07"

type WalMessageConnector interface {
	Send(walMessage *Wal2JsonMessage)
	Receive() (*Wal2JsonMessage, error)
	Close() error
}

type jsonMessage struct {
	Change    []Wal2JsonChange `json:"change"`
	Timestamp string           `json:"timestamp"`
	NextLSN   string           `json:"nextlsn"`
}

type Wal2JsonMessage struct {
	Change    []Wal2JsonChange `json:"change"`
	Timestamp time.Time        `json:"timestamp"`
	NextLSN   pglogrepl.LSN    `json:"nextlsn"`
	WALStart  pglogrepl.LSN    `json:"-"`
}

func (w *Wal2JsonMessage) UnmarshalJSON(data []byte) error {
	msg := jsonMessage{}
	var err error
	if err = json.Unmarshal(data, &msg); err != nil {
		return err
	}
	w.Change = msg.Change
	if w.Timestamp, err = time.Parse(MsgTimestampFormat, msg.Timestamp); err != nil {
		return err
	}

	if w.NextLSN, err = pglogrepl.ParseLSN(msg.NextLSN); err != nil {
		return err
	}
	return nil
}

//Wal2JsonChange defines children of root documents
type Wal2JsonChange struct {
	Kind         string          `json:"kind"`
	Schema       string          `json:"schema"`
	Table        string          `json:"table"`
	ColumnNames  []string        `json:"columnnames"`
	ColumnTypes  []string        `json:"columntypes"`
	ColumnValues []interface{}   `json:"columnvalues"`
	OldKeys      Wal2JsonOldKeys `json:"oldkeys"`
}

//Wal2JsonOldKeys defines children of OldKeys
type Wal2JsonOldKeys struct {
	KeyNames  []string      `json:"keynames"`
	KeyTypes  []string      `json:"keytypes"`
	KeyValues []interface{} `json:"keyvalues"`
}

type session struct {
	ctx                context.Context
	replConn           *pgx.Conn
	connector          WalMessageConnector
	statusInterval     time.Duration
	nextStatusDeadline time.Time
	nextLSN            pglogrepl.LSN
	slotName           string
	busyWaitInterval   time.Duration
}

func (sess *session) PgConn() *pgconn.PgConn {
	return sess.replConn.PgConn()
}
