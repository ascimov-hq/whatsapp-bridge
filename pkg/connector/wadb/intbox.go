package wadb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/exslices"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// IntboxQuery contains the queries for the intbox-specific side tables:
// the group roster (W2), group size metadata and the history bypass buffer (L1).
type IntboxQuery struct {
	BridgeID networkid.BridgeID
	*dbutil.Database
}

type RosterMember struct {
	JID          types.JID
	LID          types.JID
	Phone        string
	Name         string
	IsAdmin      bool
	IsSuperAdmin bool
	UpdatedAt    int64
}

func (rm *RosterMember) GetMassInsertValues() [6]any {
	return [6]any{rm.JID.String(), jidToDBString(rm.LID), rm.Phone, rm.Name, rm.IsAdmin, rm.IsSuperAdmin}
}

func (rm *RosterMember) Scan(row dbutil.Scannable) (*RosterMember, error) {
	var memberJID, lid string
	err := row.Scan(&memberJID, &lid, &rm.Phone, &rm.Name, &rm.IsAdmin, &rm.IsSuperAdmin, &rm.UpdatedAt)
	if err != nil {
		return nil, err
	}
	rm.JID, _ = types.ParseJID(memberJID)
	if lid != "" {
		rm.LID, _ = types.ParseJID(lid)
	}
	return rm, nil
}

type IntboxGroupInfo struct {
	TotalMemberCount  int
	SyncedMemberCount int
	UpdatedAt         int64
	SentTotalCount    int
	SentSyncedCount   int
}

type PendingHistory struct {
	LoginID networkid.UserLoginID
	ChatJID string
	Count   int
}

type HistoryCursor struct {
	Timestamp int64
	MessageID string
	SenderJID string
}

type BypassHistoryMessage struct {
	LoginID   networkid.UserLoginID
	ChatJID   string
	SenderJID string
	MessageID string
	Timestamp int64
	Data      []byte
}

func jidToDBString(jid types.JID) string {
	if jid.IsEmpty() {
		return ""
	}
	return jid.String()
}

const (
	deleteRosterQuery = `
		DELETE FROM whatsapp_intbox_roster WHERE bridge_id=$1 AND group_jid=$2
	`
	insertRosterMemberQuery = `
		INSERT INTO whatsapp_intbox_roster (bridge_id, group_jid, updated_at, member_jid, lid, phone, name, is_admin, is_super_admin)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (bridge_id, group_jid, member_jid) DO UPDATE
			SET lid=CASE WHEN excluded.lid<>'' THEN excluded.lid ELSE whatsapp_intbox_roster.lid END,
				phone=CASE WHEN excluded.phone<>'' THEN excluded.phone ELSE whatsapp_intbox_roster.phone END,
				name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE whatsapp_intbox_roster.name END,
				updated_at=excluded.updated_at
	`
	deleteRosterMemberQuery = `
		DELETE FROM whatsapp_intbox_roster
		WHERE bridge_id=$1 AND group_jid=$2 AND (member_jid=$3 OR (lid<>'' AND lid=$3) OR (phone<>'' AND phone=$4))
	`
	setRosterMemberAdminQuery = `
		UPDATE whatsapp_intbox_roster
		SET is_admin=$5, is_super_admin=CASE WHEN $5 THEN is_super_admin ELSE false END, updated_at=$6
		WHERE bridge_id=$1 AND group_jid=$2 AND (member_jid=$3 OR (lid<>'' AND lid=$3) OR (phone<>'' AND phone=$4))
	`
	getRosterQuery = `
		SELECT member_jid, lid, phone, name, is_admin, is_super_admin, updated_at
		FROM whatsapp_intbox_roster
		WHERE bridge_id=$1 AND group_jid=$2 AND member_jid>$3
		ORDER BY member_jid
		LIMIT $4
	`
	countRosterQueryTemplate = `
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN is_admin OR is_super_admin%s THEN 1 ELSE 0 END), 0)
		FROM whatsapp_intbox_roster
		WHERE bridge_id=$1 AND group_jid=$2
	`
	upsertIntboxGroupInfoQuery = `
		INSERT INTO whatsapp_intbox_group_info (bridge_id, group_jid, total_member_count, synced_member_count, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (bridge_id, group_jid) DO UPDATE
			SET total_member_count=excluded.total_member_count,
				synced_member_count=excluded.synced_member_count,
				updated_at=excluded.updated_at
	`
	getIntboxGroupInfoQuery = `
		SELECT total_member_count, synced_member_count, updated_at, sent_total_count, sent_synced_count
		FROM whatsapp_intbox_group_info
		WHERE bridge_id=$1 AND group_jid=$2
	`
	markIntboxGroupInfoSentQuery = `
		UPDATE whatsapp_intbox_group_info SET sent_total_count=$3, sent_synced_count=$4
		WHERE bridge_id=$1 AND group_jid=$2
	`
	getContactNamesQueryTemplate = `
		SELECT their_jid, COALESCE(push_name, ''), COALESCE(business_name, '')
		FROM whatsmeow_contacts
		WHERE our_jid=$1 AND their_jid IN (%s)
	`

	stashHistoryQueryTemplate = `
		INSERT INTO whatsapp_intbox_history (bridge_id, user_login_id, chat_jid, sender_jid, message_id, timestamp, data, inserted_time)
		SELECT bridge_id, user_login_id, chat_jid, sender_jid, message_id, timestamp, data, CAST($5 AS BIGINT)
		FROM whatsapp_history_sync_message
		WHERE bridge_id=$1 AND user_login_id=$2 AND chat_jid=$3 AND timestamp<$4 %s
		ORDER BY timestamp DESC, message_id DESC, sender_jid DESC
		LIMIT %d
		ON CONFLICT (bridge_id, user_login_id, chat_jid, sender_jid, message_id) DO NOTHING
	`
	deleteStashedHistorySyncQueryTemplate = `
		DELETE FROM whatsapp_history_sync_message
		WHERE bridge_id=$1 AND user_login_id=$2 AND chat_jid=$3 AND timestamp<$4 %s
	`
	countPendingHistoryQueryTemplate = `
		SELECT user_login_id, chat_jid, COUNT(*)
		FROM whatsapp_intbox_history
		WHERE bridge_id=$1 AND user_login_id IN (%s)
		GROUP BY user_login_id, chat_jid
	`
	getHistoryPageQueryTemplate = `
		SELECT user_login_id, chat_jid, sender_jid, message_id, timestamp, data
		FROM whatsapp_intbox_history
		WHERE bridge_id=$1 AND chat_jid=$2 AND (timestamp, message_id, sender_jid) > ($3, $4, $5) AND user_login_id IN (%s)
		ORDER BY timestamp, message_id, sender_jid
		LIMIT %d
	`
	ackHistoryQueryTemplate = `
		DELETE FROM whatsapp_intbox_history
		WHERE bridge_id=$1 AND chat_jid=$2 AND (timestamp, message_id, sender_jid) <= ($3, $4, $5) AND user_login_id IN (%s)
	`
	deleteExpiredHistoryQuery = `
		DELETE FROM whatsapp_intbox_history
		WHERE (bridge_id, user_login_id, chat_jid, sender_jid, message_id) IN (
			SELECT bridge_id, user_login_id, chat_jid, sender_jid, message_id FROM whatsapp_intbox_history
			WHERE bridge_id=$1 AND inserted_time<$2
			LIMIT $3
		)
	`
	deleteExpiredHistorySyncMessagesQuery = `
		DELETE FROM whatsapp_history_sync_message
		WHERE (bridge_id, user_login_id, chat_jid, sender_jid, message_id) IN (
			SELECT bridge_id, user_login_id, chat_jid, sender_jid, message_id FROM whatsapp_history_sync_message
			WHERE bridge_id=$1 AND inserted_time<$2
			LIMIT $3
		)
	`
)

var batchInsertRosterMember = dbutil.NewMassInsertBuilder[*RosterMember, [3]any](
	insertRosterMemberQuery, "($1, $2, $3, $%d, $%d, $%d, $%d, $%d, $%d)",
)

// placeholders returns a comma-separated list of n numbered placeholders starting from $start.
func placeholders(start, n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("$%d", start+i)
	}
	return strings.Join(parts, ", ")
}

func loginIDsToArgs(args []any, loginIDs []networkid.UserLoginID) []any {
	for _, loginID := range loginIDs {
		args = append(args, loginID)
	}
	return args
}

// ReplaceRoster replaces the stored participant list of a group with a fresh GetGroupInfo result.
func (iq *IntboxQuery) ReplaceRoster(ctx context.Context, groupJID types.JID, members []*RosterMember, total, synced int) error {
	now := time.Now().UnixMilli()
	return iq.DoTxn(ctx, nil, func(ctx context.Context) error {
		_, err := iq.Exec(ctx, deleteRosterQuery, iq.BridgeID, groupJID)
		if err != nil {
			return fmt.Errorf("failed to delete old roster: %w", err)
		}
		for _, chunk := range exslices.Chunk(members, 200) {
			query, params := batchInsertRosterMember.Build([3]any{iq.BridgeID, groupJID, now}, chunk)
			_, err = iq.Exec(ctx, query, params...)
			if err != nil {
				return fmt.Errorf("failed to insert roster members: %w", err)
			}
		}
		_, err = iq.Exec(ctx, upsertIntboxGroupInfoQuery, iq.BridgeID, groupJID, total, synced, now)
		if err != nil {
			return fmt.Errorf("failed to save group info: %w", err)
		}
		return nil
	})
}

// UpsertRosterMembers adds members to the roster without touching existing admin flags.
func (iq *IntboxQuery) UpsertRosterMembers(ctx context.Context, groupJID types.JID, members []*RosterMember) error {
	if len(members) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	for _, chunk := range exslices.Chunk(members, 200) {
		query, params := batchInsertRosterMember.Build([3]any{iq.BridgeID, groupJID, now}, chunk)
		_, err := iq.Exec(ctx, query, params...)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteRosterMember removes a member by JID (matching the member JID or LID) or phone number.
func (iq *IntboxQuery) DeleteRosterMember(ctx context.Context, groupJID, member types.JID, phone string) error {
	_, err := iq.Exec(ctx, deleteRosterMemberQuery, iq.BridgeID, groupJID, member.String(), phone)
	return err
}

func (iq *IntboxQuery) SetRosterMemberAdmin(ctx context.Context, groupJID, member types.JID, phone string, isAdmin bool) error {
	_, err := iq.Exec(ctx, setRosterMemberAdminQuery, iq.BridgeID, groupJID, member.String(), phone, isAdmin, time.Now().UnixMilli())
	return err
}

func (iq *IntboxQuery) GetRoster(ctx context.Context, groupJID types.JID, after string, limit int) ([]*RosterMember, error) {
	rows, err := iq.Query(ctx, getRosterQuery, iq.BridgeID, groupJID, after, limit)
	return dbutil.NewRowIterWithError(rows, func(row dbutil.Scannable) (*RosterMember, error) {
		return (&RosterMember{}).Scan(row)
	}, err).AsList()
}

// CountRoster returns the number of roster members and the number of members that the bridge
// syncs eagerly into Matrix when the member cap applies (admins, super admins and the given own JIDs).
func (iq *IntboxQuery) CountRoster(ctx context.Context, groupJID types.JID, ownJIDs ...types.JID) (total, eager int, err error) {
	args := []any{iq.BridgeID, groupJID}
	var ownClause string
	for _, own := range ownJIDs {
		if own.IsEmpty() {
			continue
		}
		args = append(args, own.String())
		ownClause += fmt.Sprintf(" OR member_jid=$%d", len(args))
	}
	err = iq.QueryRow(ctx, fmt.Sprintf(countRosterQueryTemplate, ownClause), args...).Scan(&total, &eager)
	return
}

func (iq *IntboxQuery) GetGroupInfo(ctx context.Context, groupJID types.JID) (*IntboxGroupInfo, error) {
	var info IntboxGroupInfo
	err := iq.QueryRow(ctx, getIntboxGroupInfoQuery, iq.BridgeID, groupJID).
		Scan(&info.TotalMemberCount, &info.SyncedMemberCount, &info.UpdatedAt, &info.SentTotalCount, &info.SentSyncedCount)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &info, nil
}

func (iq *IntboxQuery) UpdateGroupCounts(ctx context.Context, groupJID types.JID, total, synced int) error {
	_, err := iq.Exec(ctx, upsertIntboxGroupInfoQuery, iq.BridgeID, groupJID, total, synced, time.Now().UnixMilli())
	return err
}

func (iq *IntboxQuery) MarkGroupInfoSent(ctx context.Context, groupJID types.JID, total, synced int) error {
	_, err := iq.Exec(ctx, markIntboxGroupInfoSentQuery, iq.BridgeID, groupJID, total, synced)
	return err
}

// GetContactNames returns the public names (push name or business name) the given login knows for the given JIDs.
// It reads the whatsmeow contact table directly to avoid filling the in-memory contact cache with every group member.
// The device JID must include the device ID, as that's what whatsmeow uses as the contact store key.
func (iq *IntboxQuery) GetContactNames(ctx context.Context, deviceJID types.JID, jids []types.JID) (map[types.JID]string, error) {
	names := make(map[types.JID]string)
	if deviceJID.IsEmpty() || len(jids) == 0 {
		return names, nil
	}
	for _, chunk := range exslices.Chunk(jids, 500) {
		args := make([]any, 0, len(chunk)+1)
		args = append(args, deviceJID.String())
		for _, jid := range chunk {
			args = append(args, jid.ToNonAD().String())
		}
		rows, err := iq.Query(ctx, fmt.Sprintf(getContactNamesQueryTemplate, placeholders(2, len(chunk))), args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var jid types.JID
			var pushName, businessName string
			if err = rows.Scan(&jid, &pushName, &businessName); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if pushName != "" {
				names[jid] = pushName
			} else if businessName != "" {
				names[jid] = businessName
			}
		}
		err = rows.Close()
		if err == nil {
			err = rows.Err()
		}
		if err != nil {
			return nil, err
		}
	}
	return names, nil
}

// StashHistory moves history sync messages older than the given timestamp (in seconds) from the
// history sync buffer into the history bypass table. If notBefore is non-zero, older messages are left alone.
// Only the newest limit messages are moved, the older ones in the same range are deleted from the buffer
// without being moved (a limit of 0 or less moves nothing).
func (iq *IntboxQuery) StashHistory(ctx context.Context, loginID networkid.UserLoginID, chatJID types.JID, before, notBefore int64, limit int) (moved int64, err error) {
	args := []any{iq.BridgeID, loginID, chatJID, before}
	var extraClause string
	if notBefore > 0 {
		extraClause = "AND timestamp>=$6"
	}
	err = iq.DoTxn(ctx, nil, func(ctx context.Context) error {
		if limit > 0 {
			insertArgs := append(args, time.Now().Unix())
			if notBefore > 0 {
				insertArgs = append(insertArgs, notBefore)
			}
			res, err := iq.Exec(ctx, fmt.Sprintf(stashHistoryQueryTemplate, extraClause, limit), insertArgs...)
			if err != nil {
				return fmt.Errorf("failed to copy messages to bypass table: %w", err)
			}
			moved, _ = res.RowsAffected()
		}
		deleteArgs := args
		deleteClause := ""
		if notBefore > 0 {
			deleteArgs = append(deleteArgs, notBefore)
			deleteClause = "AND timestamp>=$5"
		}
		_, err = iq.Exec(ctx, fmt.Sprintf(deleteStashedHistorySyncQueryTemplate, deleteClause), deleteArgs...)
		if err != nil {
			return fmt.Errorf("failed to delete stashed messages from history sync buffer: %w", err)
		}
		return nil
	})
	return
}

func (iq *IntboxQuery) CountPendingHistory(ctx context.Context, loginIDs []networkid.UserLoginID) ([]*PendingHistory, error) {
	if len(loginIDs) == 0 {
		return nil, nil
	}
	args := loginIDsToArgs([]any{iq.BridgeID}, loginIDs)
	rows, err := iq.Query(ctx, fmt.Sprintf(countPendingHistoryQueryTemplate, placeholders(2, len(loginIDs))), args...)
	return dbutil.NewRowIterWithError(rows, func(row dbutil.Scannable) (*PendingHistory, error) {
		var ph PendingHistory
		return &ph, row.Scan(&ph.LoginID, &ph.ChatJID, &ph.Count)
	}, err).AsList()
}

func (iq *IntboxQuery) GetHistoryPage(ctx context.Context, loginIDs []networkid.UserLoginID, chatJID types.JID, after HistoryCursor, limit int) ([]*BypassHistoryMessage, error) {
	if len(loginIDs) == 0 {
		return nil, nil
	}
	args := loginIDsToArgs([]any{iq.BridgeID, chatJID, after.Timestamp, after.MessageID, after.SenderJID}, loginIDs)
	query := fmt.Sprintf(getHistoryPageQueryTemplate, placeholders(6, len(loginIDs)), limit)
	rows, err := iq.Query(ctx, query, args...)
	return dbutil.NewRowIterWithError(rows, func(row dbutil.Scannable) (*BypassHistoryMessage, error) {
		var msg BypassHistoryMessage
		return &msg, row.Scan(&msg.LoginID, &msg.ChatJID, &msg.SenderJID, &msg.MessageID, &msg.Timestamp, &msg.Data)
	}, err).AsList()
}

func (iq *IntboxQuery) AckHistory(ctx context.Context, loginIDs []networkid.UserLoginID, chatJID types.JID, upTo HistoryCursor) (int64, error) {
	if len(loginIDs) == 0 {
		return 0, nil
	}
	args := loginIDsToArgs([]any{iq.BridgeID, chatJID, upTo.Timestamp, upTo.MessageID, upTo.SenderJID}, loginIDs)
	res, err := iq.Exec(ctx, fmt.Sprintf(ackHistoryQueryTemplate, placeholders(6, len(loginIDs))), args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (iq *IntboxQuery) deleteExpiredInBatches(ctx context.Context, query string, olderThan time.Time, batchSize int) (total int64, err error) {
	for {
		var res sql.Result
		res, err = iq.Exec(ctx, query, iq.BridgeID, olderThan.Unix(), batchSize)
		if err != nil {
			return
		}
		affected, _ := res.RowsAffected()
		total += affected
		if affected < int64(batchSize) {
			return
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// DeleteExpiredHistory deletes history bypass rows that were stored before the given time and never acknowledged.
func (iq *IntboxQuery) DeleteExpiredHistory(ctx context.Context, olderThan time.Time) (int64, error) {
	return iq.deleteExpiredInBatches(ctx, deleteExpiredHistoryQuery, olderThan, 5000)
}

// DeleteExpiredHistorySyncMessages deletes rows from the history sync buffer that were stored before the given time.
func (iq *IntboxQuery) DeleteExpiredHistorySyncMessages(ctx context.Context, olderThan time.Time) (int64, error) {
	return iq.deleteExpiredInBatches(ctx, deleteExpiredHistorySyncMessagesQuery, olderThan, 5000)
}
