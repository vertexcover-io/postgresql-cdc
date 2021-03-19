package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

var activeSessions = make(map[string]*session)

func generateSlotName() string {
	return fmt.Sprintf("slot_%d", time.Now().Unix())
}

func sendStatusUpdate(sess *session) error {
	err := pglogrepl.SendStandbyStatusUpdate(
		context.Background(), sess.PgConn(),
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: sess.nextLSN,
			WALFlushPosition: sess.nextLSN,
			WALApplyPosition: sess.nextLSN,
		},
	)
	if err != nil {
		return err
	}
	logger.Debug("Sent Standby status message. Apply Position: %s", sess.nextLSN)
	sess.nextStatusDeadline = time.Now().Add(sess.statusInterval)
	return nil
}

func handleKeepAliveMessage(
	msg *pgproto3.CopyData,
	sess *session,
) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
	if err != nil {
		return errors.Wrap(err, "Failed while parsing primary keep alive message")
	}
	// logger.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

	if pkm.ReplyRequested {
		sess.nextStatusDeadline = time.Time{}
	}
	return nil
}

func logWALMessage(logFile string, walMessage string) error {
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to open log file: %s", logFile)
	}
	defer f.Close()
	if _, err := f.WriteString(walMessage + "\n"); err != nil {
		return errors.Wrap(err, "Unable to log message to file")
	}
	return nil
}

func handleWALMessage(
	msg *pgproto3.CopyData,
	sess *session,
) (*Wal2JsonMessage, error) {
	xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
	if err != nil {
		return nil, errors.Wrap(err, "Failed to parse xlog message")
	}
	logWALMessage("wal.txt", string(xld.WALData))

	walMessage := &Wal2JsonMessage{}
	err = json.Unmarshal(xld.WALData, walMessage)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to parse the wal message: ")
	}
	walMessage.WALStart = xld.WALStart
	return walMessage, nil
}

func closeSession(sess *session) error {
	if err := sendStatusUpdate(sess); err != nil {
		logger.Warn("Unable to send status update while closing session", err)
	}
	pglogrepl.DropReplicationSlot(
		context.Background(), sess.PgConn(), sess.slotName, pglogrepl.DropReplicationSlotOptions{},
	)
	delete(activeSessions, sess.slotName)
	sess.replConn.Close(context.Background())
	return nil
}

func fetchMessageFromPostgres(
	sess *session,
) error {
	ctx, cancel := context.WithDeadline(context.Background(), sess.nextStatusDeadline)
	msg, err := sess.PgConn().ReceiveMessage(ctx)
	cancel()
	if err != nil {
		if pgconn.Timeout(err) {
			logger.Debug("Error while receiving message", err)
			return nil
		}
		return err
	}
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			return handleKeepAliveMessage(msg, sess)
		case pglogrepl.XLogDataByteID:
			walMessage, err := handleWALMessage(msg, sess)
			if err != nil {
				return err
			}
			sess.connector.Send(walMessage)
			return nil
		default:
			return fmt.Errorf("Received unexpected message: %+v", msg)
		}
	}
	return nil
}

func streamReplicationMessages(
	ctx context.Context,
	sess *session,
) error {
	for {
		if time.Now().After(sess.nextStatusDeadline) {
			if err := sendStatusUpdate(sess); err != nil {
				logger.Error("Failed to send status update", err)
			}
		}
		select {
		case <-ctx.Done():
			logger.Info("Context is cancelled, closing session")
			closeSession(sess)
		default:

			if err := fetchMessageFromPostgres(sess); err != nil {
				logger.Error("Failed while fetching message from postgres", err)
			}
		}
	}
}

func replicationSlotExists(conn *pgx.Conn, slotName string) (bool, error) {
	var exists bool
	err := conn.QueryRow(
		context.Background(),
		"SELECT EXISTS(Select 1 from pg_replication_slots where slot_name = $1)",
		slotName,
	).Scan(&exists)

	if err != nil {
		return false, err
	}

	return exists, nil
}

func initiateReplication(
	ctx context.Context,
	conn *pgx.Conn,
	sess *session,
) error {

	exists, err := replicationSlotExists(conn, sess.slotName)
	if err != nil {
		return errors.Wrapf(err, "Unable to check if replication slot: %s exist", sess.slotName)
	}

	if exists {
		logger.Infof("Replication Slot: %s Already Exists. No need to start replication", sess.slotName)
	} else {

		if _, err := pglogrepl.CreateReplicationSlot(
			ctx, sess.PgConn(), sess.slotName, "wal2json",
			pglogrepl.CreateReplicationSlotOptions{},
		); err != nil {
			return errors.Wrap(err, "CreateReplicationSlot failed")
		}
		logger.Debugf("Created temporary replication slot: %s", sess.slotName)
	}
	err = pglogrepl.StartReplication(
		ctx, sess.PgConn(),
		sess.slotName, pglogrepl.LSN(sess.nextLSN),
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"\"include-lsn\" 'on'",
			"\"pretty-print\" 'off'",
			"\"include-timestamp\" 'on'",
			"\"filter-tables\" 'public.product_execution_flow, public.current_lsn_status'",
		}},
	)
	if err != nil {
		return errors.Wrap(err, "Start replication log failed")
	}
	logger.Infof("Logical replication started on slot: %s", sess.slotName)
	return nil
}

// delete all old slots that were created by us
func deleteAllSlots(conn *pgx.Conn, replConn *pgx.Conn) error {
	rows, err := conn.Query(context.Background(), "SELECT slot_name FROM pg_replication_slots")
	if err != nil {
		return err
	}
	for rows.Next() {
		var slotName string
		rows.Scan(&slotName)

		// only delete slots created by this program
		if !strings.Contains(slotName, "slot_") {
			continue
		}
		logger.Debugf("Deleting replication slot %s", slotName)
		err := pglogrepl.DropReplicationSlot(
			context.Background(), replConn.PgConn(), slotName, pglogrepl.DropReplicationSlotOptions{},
		)
		if err != nil {
			return errors.Wrapf(err, "could not delete slot %s", slotName)
		}
	}
	return nil
}

func StartSession(
	ctx context.Context,
	loggr Logger,
	dsn string,
	identifier string,
	connector CDCStreamer,
	startLSN pglogrepl.LSN,
	busyWaitInterval time.Duration,
	keepAlive time.Duration) (string, error) {

	if loggr != nil {
		logger = loggr
	}

	conn, err := pgx.Connect(
		ctx, dsn,
	)

	if err != nil {
		return "", errors.Wrap(err, "Failed to connect to postgresql server in default context")
	}

	// Making replication conn
	replConn, err := pgx.Connect(
		ctx,
		fmt.Sprintf("%s replication=database", dsn),
	)

	if err != nil {
		return "", errors.Wrap(err, "Failed to connect to postgresql server in replication context")
	}

	var slotName string
	if identifier == "" {
		slotName = generateSlotName()
	} else {
		slotName = identifier
	}

	sess := &session{
		ctx:              ctx,
		slotName:         slotName,
		replConn:         replConn,
		busyWaitInterval: busyWaitInterval,
		statusInterval:   keepAlive,
		connector:        connector,
	}
	activeSessions[sess.slotName] = sess

	if startLSN == 0 {
		sysident, err := pglogrepl.IdentifySystem(context.Background(), sess.PgConn())
		if err != nil {
			return sess.slotName, errors.Wrap(
				err, "Unable to find out the current lsn of the db. Cannot start cdc")
		}
		sess.nextLSN = sysident.XLogPos

		logger.Debug("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	} else {
		sess.nextLSN = startLSN
	}
	initiateReplication(ctx, conn, sess)
	sess.nextStatusDeadline = time.Now().Add(sess.statusInterval)
	go streamReplicationMessages(ctx, sess)
	return sess.slotName, nil
}

func OnMessageProcessed(
	ctx context.Context,
	name string,
	walMessage *Wal2JsonMessage,
) error {
	var sess *session
	var ok bool
	if sess, ok = activeSessions[name]; !ok {
		return fmt.Errorf("No active session found with name: %s", name)
	}
	sess.nextLSN = walMessage.NextLSN
	sess.nextStatusDeadline = time.Now()
	//logger.Infof("Processed Message with LSN: %v, Updating the applied LSN to: %s", walMessage.WALStart, walMessage.NextLSN)
	return nil
}
