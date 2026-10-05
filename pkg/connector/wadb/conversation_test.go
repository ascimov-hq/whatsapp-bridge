package wadb

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow/types"
)

const conversationTestSchema = `
CREATE TABLE user_login (
    bridge_id TEXT NOT NULL,
    id        TEXT NOT NULL,
    PRIMARY KEY (bridge_id, id)
);
CREATE TABLE whatsapp_history_sync_conversation (
    bridge_id                    TEXT    NOT NULL,
    user_login_id                TEXT    NOT NULL,
    chat_jid                     TEXT    NOT NULL,

    last_message_timestamp       BIGINT,
    archived                     BOOLEAN,
    pinned                       BOOLEAN,
    mute_end_time                BIGINT,
    end_of_history_transfer_type INTEGER,
    ephemeral_expiration         INTEGER,
    ephemeral_setting_timestamp  BIGINT,
    marked_as_unread             BOOLEAN,
    unread_count                 INTEGER,
    synced_login_ts              BIGINT,

    PRIMARY KEY (bridge_id, user_login_id, chat_jid),
    CONSTRAINT whatsapp_history_sync_conversation_user_login_fkey FOREIGN KEY (bridge_id, user_login_id)
        REFERENCES user_login (bridge_id, id) ON UPDATE CASCADE ON DELETE CASCADE
);
INSERT INTO user_login (bridge_id, id) VALUES ('wa', 'login1'), ('wa', 'login2');
`

// newConversationTestDB opens an in-memory SQLite database, or the Postgres database in INTBOX_TEST_PG_DSN.
func newConversationTestDB(t *testing.T) *ConversationQuery {
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
		if _, err = db.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
			t.Fatalf("failed to reset schema: %v", err)
		}
	}
	if _, err = db.Exec(ctx, conversationTestSchema); err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}
	return &ConversationQuery{
		BridgeID: "wa",
		QueryHelper: dbutil.MakeQueryHelper(db, func(_ *dbutil.QueryHelper[*Conversation]) *Conversation {
			return &Conversation{}
		}),
	}
}

func candidateJIDs(cands []*ImportCandidate) []string {
	jids := make([]string, len(cands))
	for i, cand := range cands {
		jids[i] = cand.ChatJID.User
	}
	return jids
}

func conversationJIDs(convs []*Conversation) []string {
	jids := make([]string, len(convs))
	for i, conv := range convs {
		jids[i] = conv.ChatJID.User
	}
	return jids
}

// TestImportCandidatesWithEpoch covers the history import key of a re-pair (intbox.reimport_after): the
// import epoch is kept, conversations imported before the re-pair are still returned (they get a catch-up
// backfill) but flagged as imported in the epoch, and the per-login room cap counts the whole epoch.
func TestImportCandidatesWithEpoch(t *testing.T) {
	cq := newConversationTestDB(t)
	ctx := context.Background()
	group := func(id string) types.JID { return types.NewJID(id, types.GroupServer) }
	// Most recent first: a, b, c, d
	for i, id := range []string{"d", "c", "b", "a"} {
		err := cq.Put(ctx, &Conversation{UserLoginID: "login1", ChatJID: group(id), LastMessageTimestamp: time.Unix(int64(1000*(i+1)), 0)})
		if err != nil {
			t.Fatalf("failed to insert conversation: %v", err)
		}
	}
	// A conversation of another login must never be returned
	if err := cq.Put(ctx, &Conversation{UserLoginID: "login2", ChatJID: group("x"), LastMessageTimestamp: time.Unix(9000, 0)}); err != nil {
		t.Fatalf("failed to insert conversation: %v", err)
	}
	epoch := jsontime.UInt(10_000)  // first pairing (import epoch starts)
	repair := jsontime.UInt(20_000) // re-pair within reimport_after, epoch carried
	// The first pairing imported a and b
	for _, id := range []string{"a", "b"} {
		if err := cq.MarkSynced(ctx, "login1", group(id), epoch); err != nil {
			t.Fatalf("failed to mark synced: %v", err)
		}
	}
	if count, err := cq.CountSynced(ctx, "login1", epoch); err != nil || count != 2 {
		t.Fatalf("expected 2 synced conversations in the epoch, got %d (%v)", count, err)
	}
	// Old semantics (key = epoch): only conversations never imported in the epoch
	recent, err := cq.GetRecent(ctx, "login1", -1, epoch)
	if err != nil {
		t.Fatalf("GetRecent failed: %v", err)
	} else if got := conversationJIDs(recent); !slices.Equal(got, []string{"c", "d"}) {
		t.Fatalf("unexpected GetRecent result with the epoch: %v", got)
	}
	// Re-pair: everything not handled in this pairing, most recent first, with a and b flagged as imported
	cands, err := cq.GetImportCandidates(ctx, "login1", -1, repair)
	if err != nil {
		t.Fatalf("GetImportCandidates failed: %v", err)
	} else if got := candidateJIDs(cands); !slices.Equal(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("unexpected candidates: %v", got)
	}
	for _, cand := range cands {
		imported := cand.ChatJID.User == "a" || cand.ChatJID.User == "b"
		if cand.SyncedSince(epoch) != imported {
			t.Errorf("%s: expected SyncedSince(epoch)=%t", cand.ChatJID.User, imported)
		} else if cand.SyncedSince(repair) {
			t.Errorf("%s: shouldn't be synced in the current pairing", cand.ChatJID.User)
		}
	}
	if limited, err := cq.GetImportCandidates(ctx, "login1", 2, repair); err != nil || !slices.Equal(candidateJIDs(limited), []string{"a", "b"}) {
		t.Fatalf("unexpected limited candidates: %v (%v)", candidateJIDs(limited), err)
	}
	// The re-pair catches up a and creates c
	for _, id := range []string{"a", "c"} {
		if err = cq.MarkSynced(ctx, "login1", group(id), repair); err != nil {
			t.Fatalf("failed to mark synced: %v", err)
		}
	}
	if count, err := cq.CountSynced(ctx, "login1", epoch); err != nil || count != 3 {
		t.Fatalf("expected 3 rooms in the epoch, got %d (%v)", count, err)
	}
	if cands, err = cq.GetImportCandidates(ctx, "login1", -1, repair); err != nil || !slices.Equal(candidateJIDs(cands), []string{"b", "d"}) {
		t.Fatalf("unexpected candidates after the re-pair round: %v (%v)", candidateJIDs(cands), err)
	} else if !cands[0].SyncedSince(epoch) || cands[1].SyncedSince(epoch) {
		t.Fatal("unexpected epoch flags after the re-pair round")
	}
	// A pairing after reimport_after starts a new epoch: nothing counts, everything is imported again
	fresh := jsontime.UInt(900_000)
	if count, err := cq.CountSynced(ctx, "login1", fresh); err != nil || count != 0 {
		t.Fatalf("expected 0 rooms in a new epoch, got %d (%v)", count, err)
	}
	if cands, err = cq.GetImportCandidates(ctx, "login1", -1, fresh); err != nil || len(cands) != 4 {
		t.Fatalf("expected all 4 conversations in a new epoch, got %v (%v)", candidateJIDs(cands), err)
	}
	for _, cand := range cands {
		if cand.SyncedSince(fresh) {
			t.Errorf("%s: flagged as imported in a new epoch", cand.ChatJID.User)
		}
	}
}

// TestImportCandidatesMatchGetRecentForExistingLogins makes sure that logins created before import epochs
// (epoch = LoggedInAt, nothing carried) get exactly the conversations they got before the upgrade.
func TestImportCandidatesMatchGetRecentForExistingLogins(t *testing.T) {
	cq := newConversationTestDB(t)
	ctx := context.Background()
	loggedInAt := jsontime.UInt(50_000)
	for i := 0; i < 6; i++ {
		jid := types.NewJID(string(rune('a'+i)), types.DefaultUserServer)
		if err := cq.Put(ctx, &Conversation{UserLoginID: "login1", ChatJID: jid, LastMessageTimestamp: time.Unix(int64(100*i+1), 0)}); err != nil {
			t.Fatalf("failed to insert conversation: %v", err)
		}
		if i%2 == 0 {
			if err := cq.MarkSynced(ctx, "login1", jid, loggedInAt); err != nil {
				t.Fatalf("failed to mark synced: %v", err)
			}
		}
	}
	for _, limit := range []int{-1, 2, 10} {
		recent, err := cq.GetRecent(ctx, "login1", limit, loggedInAt)
		if err != nil {
			t.Fatalf("GetRecent failed: %v", err)
		}
		cands, err := cq.GetImportCandidates(ctx, "login1", limit, loggedInAt)
		if err != nil {
			t.Fatalf("GetImportCandidates failed: %v", err)
		}
		if !slices.Equal(conversationJIDs(recent), candidateJIDs(cands)) {
			t.Fatalf("limit %d: GetRecent %v != GetImportCandidates %v", limit, conversationJIDs(recent), candidateJIDs(cands))
		}
		for _, cand := range cands {
			if cand.SyncedSince(loggedInAt) {
				t.Fatalf("limit %d: %s is flagged as imported", limit, cand.ChatJID.User)
			}
		}
	}
}
