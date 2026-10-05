package wadb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const intboxTestBaseSchema = `
CREATE TABLE user_login (
    bridge_id TEXT NOT NULL,
    id        TEXT NOT NULL,
    PRIMARY KEY (bridge_id, id)
);
CREATE TABLE whatsapp_history_sync_message (
    bridge_id     TEXT   NOT NULL,
    user_login_id TEXT   NOT NULL,
    chat_jid      TEXT   NOT NULL,
    sender_jid    TEXT   NOT NULL,
    message_id    TEXT   NOT NULL,
    timestamp     BIGINT NOT NULL,
    data          bytea  NOT NULL,
    inserted_time BIGINT NOT NULL,
    PRIMARY KEY (bridge_id, user_login_id, chat_jid, sender_jid, message_id)
);
CREATE TABLE whatsmeow_contacts (
    our_jid       TEXT,
    their_jid     TEXT,
    first_name    TEXT,
    full_name     TEXT,
    push_name     TEXT,
    business_name TEXT,
    PRIMARY KEY (our_jid, their_jid)
);
INSERT INTO user_login (bridge_id, id) VALUES ('wa', 'login1'), ('wa', 'login2');
`

// newIntboxTestDB opens an in-memory SQLite database, or the Postgres database in INTBOX_TEST_PG_DSN.
func newIntboxTestDB(t *testing.T) *IntboxQuery {
	t.Helper()
	var db *dbutil.Database
	var err error
	if dsn := os.Getenv("INTBOX_TEST_PG_DSN"); dsn != "" {
		db, err = dbutil.NewWithDialect(dsn, "postgres")
	} else {
		db, err = dbutil.NewWithDialect("file:"+t.Name()+"?mode=memory&cache=shared", "sqlite3")
	}
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if db.Dialect == dbutil.Postgres {
		_, err = db.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
		if err != nil {
			t.Fatalf("failed to reset schema: %v", err)
		}
	}
	upgrade, err := os.ReadFile("upgrades/10-intbox-roster-history.sql")
	if err != nil {
		t.Fatalf("failed to read upgrade: %v", err)
	}
	for _, query := range []string{intboxTestBaseSchema, string(upgrade)} {
		if _, err = db.Exec(ctx, query); err != nil {
			t.Fatalf("failed to create schema: %v", err)
		}
	}
	return &IntboxQuery{BridgeID: "wa", Database: db}
}

func TestIntboxRoster(t *testing.T) {
	iq := newIntboxTestDB(t)
	ctx := context.Background()
	group := types.NewJID("123", types.GroupServer)
	own := types.NewJID("911", types.DefaultUserServer)
	members := []*RosterMember{
		{JID: own, Phone: "+911"},
		{JID: types.NewJID("912", types.DefaultUserServer), Phone: "+912", Name: "Admin", IsAdmin: true},
		{JID: types.NewJID("913", types.DefaultUserServer), Phone: "+913", IsAdmin: true, IsSuperAdmin: true},
		{JID: types.NewJID("555", types.HiddenUserServer), LID: types.NewJID("555", types.HiddenUserServer)},
	}
	if err := iq.ReplaceRoster(ctx, group, members, 4, 3); err != nil {
		t.Fatalf("ReplaceRoster: %v", err)
	}
	// Replacing again must not duplicate
	if err := iq.ReplaceRoster(ctx, group, members, 4, 3); err != nil {
		t.Fatalf("ReplaceRoster again: %v", err)
	}
	page, err := iq.GetRoster(ctx, group, "", 3)
	if err != nil || len(page) != 3 {
		t.Fatalf("GetRoster first page: %v %d", err, len(page))
	}
	rest, err := iq.GetRoster(ctx, group, page[2].JID.String(), 3)
	if err != nil || len(rest) != 1 {
		t.Fatalf("GetRoster second page: %v %d", err, len(rest))
	}
	if rest[0].LID.IsEmpty() && page[0].LID.IsEmpty() {
		t.Fatalf("LID member lost its LID")
	}
	total, eager, err := iq.CountRoster(ctx, group, own, types.NewJID("777", types.HiddenUserServer))
	if err != nil || total != 4 || eager != 3 {
		t.Fatalf("CountRoster: %v total=%d eager=%d", err, total, eager)
	}
	if err = iq.UpsertRosterMembers(ctx, group, []*RosterMember{{JID: types.NewJID("914", types.DefaultUserServer), Phone: "+914"}, {JID: own}}); err != nil {
		t.Fatalf("UpsertRosterMembers: %v", err)
	}
	if err = iq.SetRosterMemberAdmin(ctx, group, types.NewJID("914", types.DefaultUserServer), "+914", true); err != nil {
		t.Fatalf("SetRosterMemberAdmin: %v", err)
	}
	if err = iq.SetRosterMemberAdmin(ctx, group, types.NewJID("913", types.DefaultUserServer), "+913", false); err != nil {
		t.Fatalf("SetRosterMemberAdmin demote: %v", err)
	}
	if err = iq.DeleteRosterMember(ctx, group, types.NewJID("999", types.HiddenUserServer), "+912"); err != nil {
		t.Fatalf("DeleteRosterMember: %v", err)
	}
	total, eager, err = iq.CountRoster(ctx, group, own)
	if err != nil || total != 4 || eager != 2 {
		t.Fatalf("CountRoster after changes: %v total=%d eager=%d", err, total, eager)
	}
	all, _ := iq.GetRoster(ctx, group, "", 10)
	for _, member := range all {
		if member.JID == own && member.Phone != "+911" {
			t.Fatalf("upsert overwrote phone with empty value")
		}
	}
	info, err := iq.GetGroupInfo(ctx, group)
	if err != nil || info == nil || info.TotalMemberCount != 4 || info.SyncedMemberCount != 3 || info.SentTotalCount != -1 {
		t.Fatalf("GetGroupInfo: %v %+v", err, info)
	}
	if err = iq.UpdateGroupCounts(ctx, group, 5, 2); err != nil {
		t.Fatalf("UpdateGroupCounts: %v", err)
	}
	if err = iq.MarkGroupInfoSent(ctx, group, 5, 2); err != nil {
		t.Fatalf("MarkGroupInfoSent: %v", err)
	}
	info, _ = iq.GetGroupInfo(ctx, group)
	if info.SentTotalCount != 5 || info.SentSyncedCount != 2 || info.TotalMemberCount != 5 {
		t.Fatalf("group info after update: %+v", info)
	}
	if missing, err := iq.GetGroupInfo(ctx, types.NewJID("456", types.GroupServer)); err != nil || missing != nil {
		t.Fatalf("GetGroupInfo of unknown group: %v %+v", err, missing)
	}

	device := types.JID{User: "911", Device: 12, Server: types.DefaultUserServer}
	_, err = iq.Exec(ctx, `INSERT INTO whatsmeow_contacts (our_jid, their_jid, push_name, business_name) VALUES ($1, '912@s.whatsapp.net', 'Push', NULL), ($1, '913@s.whatsapp.net', NULL, 'Biz')`, device.String())
	if err != nil {
		t.Fatalf("insert contacts: %v", err)
	}
	names, err := iq.GetContactNames(ctx, device, []types.JID{types.NewJID("912", types.DefaultUserServer), types.NewJID("913", types.DefaultUserServer), types.NewJID("914", types.DefaultUserServer)})
	if err != nil || len(names) != 2 || names[types.NewJID("912", types.DefaultUserServer)] != "Push" || names[types.NewJID("913", types.DefaultUserServer)] != "Biz" {
		t.Fatalf("GetContactNames: %v %v", err, names)
	}
}

func TestIntboxHistory(t *testing.T) {
	iq := newIntboxTestDB(t)
	ctx := context.Background()
	chat := types.NewJID("123", types.GroupServer)
	now := time.Now().Unix()
	for i := 0; i < 10; i++ {
		_, err := iq.Exec(ctx, `
			INSERT INTO whatsapp_history_sync_message (bridge_id, user_login_id, chat_jid, sender_jid, message_id, timestamp, data, inserted_time)
			VALUES ('wa', $1, $2, '911@s.whatsapp.net', $3, $4, $5, $6)
		`, "login1", chat.String(), string(rune('A'+i)), int64(100+i), []byte{byte(i)}, now-int64(i))
		if err != nil {
			t.Fatalf("insert history sync message: %v", err)
		}
	}
	// Messages 100..104 are older than the oldest message sent to Matrix (105), but 100 and 101 are outside the age window
	moved, err := iq.StashHistory(ctx, "login1", chat, 105, 102, 1000)
	if err != nil || moved != 3 {
		t.Fatalf("StashHistory: %v moved=%d", err, moved)
	}
	moved, err = iq.StashHistory(ctx, "login1", chat, 105, 0, 1000)
	if err != nil || moved != 2 {
		t.Fatalf("StashHistory without window: %v moved=%d", err, moved)
	}
	var remaining int
	_ = iq.QueryRow(ctx, "SELECT COUNT(*) FROM whatsapp_history_sync_message").Scan(&remaining)
	if remaining != 5 {
		t.Fatalf("expected 5 remaining buffer rows, got %d", remaining)
	}
	pending, err := iq.CountPendingHistory(ctx, []networkid.UserLoginID{"login1", "login2"})
	if err != nil || len(pending) != 1 || pending[0].Count != 5 || pending[0].ChatJID != chat.String() {
		t.Fatalf("CountPendingHistory: %v %+v", err, pending)
	}
	page, err := iq.GetHistoryPage(ctx, []networkid.UserLoginID{"login1", "login2"}, chat, HistoryCursor{Timestamp: -1}, 3)
	if err != nil || len(page) != 3 || page[0].Timestamp != 100 || page[2].Timestamp != 102 || page[1].MessageID != "B" {
		t.Fatalf("GetHistoryPage: %v %d", err, len(page))
	}
	last := page[2]
	next, err := iq.GetHistoryPage(ctx, []networkid.UserLoginID{"login1"}, chat, HistoryCursor{Timestamp: last.Timestamp, MessageID: last.MessageID, SenderJID: last.SenderJID}, 3)
	if err != nil || len(next) != 2 || next[0].Timestamp != 103 {
		t.Fatalf("GetHistoryPage second page: %v %d", err, len(next))
	}
	deleted, err := iq.AckHistory(ctx, []networkid.UserLoginID{"login1"}, chat, HistoryCursor{Timestamp: last.Timestamp, MessageID: last.MessageID, SenderJID: last.SenderJID})
	if err != nil || deleted != 3 {
		t.Fatalf("AckHistory: %v deleted=%d", err, deleted)
	}
	deleted, err = iq.AckHistory(ctx, []networkid.UserLoginID{"login2"}, chat, HistoryCursor{Timestamp: 1000})
	if err != nil || deleted != 0 {
		t.Fatalf("AckHistory for other login: %v deleted=%d", err, deleted)
	}
	deleted, err = iq.DeleteExpiredHistory(ctx, time.Now().Add(time.Hour))
	if err != nil || deleted != 2 {
		t.Fatalf("DeleteExpiredHistory: %v deleted=%d", err, deleted)
	}
	deleted, err = iq.DeleteExpiredHistorySyncMessages(ctx, time.Unix(now-6, 0))
	if err != nil || deleted != 3 {
		// Remaining buffer rows have inserted_time now-5..now-9 (messages 105..109), three of them are older than now-6
		t.Fatalf("DeleteExpiredHistorySyncMessages: %v deleted=%d", err, deleted)
	}
}

// TestIntboxHistoryLimit checks that the bypass only keeps the newest messages up to the limit and drops the
// older ones in the window, so that Matrix and the bypass together stay within backfill.max_initial_messages.
func TestIntboxHistoryLimit(t *testing.T) {
	iq := newIntboxTestDB(t)
	ctx := context.Background()
	chat := types.NewJID("123", types.GroupServer)
	other := types.NewJID("456", types.GroupServer)
	now := time.Now().Unix()
	for _, chatJID := range []types.JID{chat, other} {
		for i := 0; i < 10; i++ {
			_, err := iq.Exec(ctx, `
				INSERT INTO whatsapp_history_sync_message (bridge_id, user_login_id, chat_jid, sender_jid, message_id, timestamp, data, inserted_time)
				VALUES ('wa', $1, $2, '911@s.whatsapp.net', $3, $4, $5, $6)
			`, "login1", chatJID.String(), string(rune('A'+i)), int64(100+i), []byte{byte(i)}, now)
			if err != nil {
				t.Fatalf("insert history sync message: %v", err)
			}
		}
	}
	// Messages 108 and 109 went to Matrix. 101..107 are older and inside the age window, but only the newest 3 fit.
	moved, err := iq.StashHistory(ctx, "login1", chat, 108, 101, 3)
	if err != nil || moved != 3 {
		t.Fatalf("StashHistory with limit: %v moved=%d", err, moved)
	}
	page, err := iq.GetHistoryPage(ctx, []networkid.UserLoginID{"login1"}, chat, HistoryCursor{Timestamp: -1}, 10)
	if err != nil || len(page) != 3 || page[0].Timestamp != 105 || page[2].Timestamp != 107 {
		t.Fatalf("GetHistoryPage after limited stash: %v %d", err, len(page))
	}
	var remaining int
	_ = iq.QueryRow(ctx, "SELECT COUNT(*) FROM whatsapp_history_sync_message WHERE chat_jid=$1", chat.String()).Scan(&remaining)
	if remaining != 3 {
		// 100 is outside the age window and 108, 109 are deleted by the backfill itself, the other 7 must be gone
		t.Fatalf("expected 3 remaining buffer rows, got %d", remaining)
	}
	// A limit of 0 (Matrix already got max_initial_messages) moves nothing, but still drops the window
	moved, err = iq.StashHistory(ctx, "login1", other, 108, 101, 0)
	if err != nil || moved != 0 {
		t.Fatalf("StashHistory with zero limit: %v moved=%d", err, moved)
	}
	_ = iq.QueryRow(ctx, "SELECT COUNT(*) FROM whatsapp_history_sync_message WHERE chat_jid=$1", other.String()).Scan(&remaining)
	if remaining != 3 {
		t.Fatalf("expected 3 remaining buffer rows in other chat, got %d", remaining)
	}
	pending, err := iq.CountPendingHistory(ctx, []networkid.UserLoginID{"login1"})
	if err != nil || len(pending) != 1 || pending[0].Count != 3 || pending[0].ChatJID != chat.String() {
		t.Fatalf("CountPendingHistory: %v %+v", err, pending)
	}
}

// L7-1: the admission lanes count a login's buffered history sync messages, capped by the limit.
func TestCountHistorySyncMessages(t *testing.T) {
	iq := newIntboxTestDB(t)
	mq := &MessageQuery{BridgeID: "wa", Database: iq.Database}
	ctx := context.Background()
	now := time.Now().Unix()
	for i, row := range []struct{ bridge, login, chat string }{
		{"wa", "login1", "1@g.us"}, {"wa", "login1", "1@g.us"}, {"wa", "login1", "2@g.us"}, {"wa", "login1", "3@g.us"},
		{"wa", "login1", "3@g.us"}, {"wa", "login2", "1@g.us"}, {"other", "login1", "1@g.us"},
	} {
		_, err := iq.Exec(ctx, `
			INSERT INTO whatsapp_history_sync_message (bridge_id, user_login_id, chat_jid, sender_jid, message_id, timestamp, data, inserted_time)
			VALUES ($1, $2, $3, '911@s.whatsapp.net', $4, $5, $6, $7)
		`, row.bridge, row.login, row.chat, string(rune('A'+i)), int64(100+i), []byte{byte(i)}, now)
		if err != nil {
			t.Fatalf("insert history sync message: %v", err)
		}
	}
	for _, tc := range []struct {
		login        networkid.UserLoginID
		limit, count int
	}{
		{"login1", 100, 5}, {"login1", 5, 5}, {"login1", 3, 3}, {"login2", 100, 1}, {"login3", 100, 0},
	} {
		count, err := mq.CountHistorySyncMessages(ctx, tc.login, tc.limit)
		if err != nil || count != tc.count {
			t.Errorf("CountHistorySyncMessages(%s, %d) = %d, %v; expected %d", tc.login, tc.limit, count, err, tc.count)
		}
	}
}

func TestIntboxUpgrade(t *testing.T) {
	ctx := context.Background()
	for _, fromV9 := range []bool{false, true} {
		db, err := dbutil.NewWithDialect(fmt.Sprintf("file:%s-%t?mode=memory&cache=shared", t.Name(), fromV9), "sqlite3")
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		_, err = db.Exec(ctx, `
			CREATE TABLE portal (bridge_id TEXT, id TEXT, receiver TEXT, PRIMARY KEY (bridge_id, id, receiver));
			CREATE TABLE message (bridge_id TEXT, room_receiver TEXT, id TEXT, part_id TEXT, mxid TEXT, PRIMARY KEY (bridge_id, room_receiver, id, part_id));
			CREATE TABLE kv_store (bridge_id TEXT, key TEXT, value TEXT, PRIMARY KEY (bridge_id, key));
		`)
		if err == nil && fromV9 {
			_, err = db.Exec(ctx, intboxTestBaseSchema+`
				CREATE TABLE whatsapp_version (version INTEGER, compat INTEGER);
				INSERT INTO whatsapp_version (version, compat) VALUES (9, 3);
			`)
		} else if err == nil {
			_, err = db.Exec(ctx, "CREATE TABLE user_login (bridge_id TEXT NOT NULL, id TEXT NOT NULL, PRIMARY KEY (bridge_id, id))")
		}
		if err != nil {
			t.Fatalf("failed to create base tables: %v", err)
		}
		err = New("wa", db, zerolog.Nop()).Upgrade(ctx)
		if err != nil {
			t.Fatalf("upgrade (from v9: %t) failed: %v", fromV9, err)
		}
		var version int
		if err = db.QueryRow(ctx, "SELECT version FROM whatsapp_version").Scan(&version); err != nil || version != 10 {
			t.Fatalf("unexpected version after upgrade (from v9: %t): %v %d", fromV9, err, version)
		}
		if _, err = db.Exec(ctx, "SELECT COUNT(*) FROM whatsapp_intbox_history"); err != nil {
			t.Fatalf("bypass table missing after upgrade (from v9: %t): %v", fromV9, err)
		}
		_ = db.Close()
	}
}
