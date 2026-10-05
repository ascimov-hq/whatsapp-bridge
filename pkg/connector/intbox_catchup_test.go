package connector

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/wadb"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// These tests run createPortalsFromHistorySync for a re-pair within intbox.reimport_after against a real bridgev2
// bridge (portal event loops included) and the connector's own tables, to check when HistorySyncPortalsNeedCreating
// is cleared. The flag is what makes historySyncLoop dispatch the history sync again after a restart.

// catchUpTestMatrix is a Matrix connector without double puppeting whose bot can always invite. Queuing a remote
// event for a portal with a room ensures that the login's user is in it (UserLogin.MarkInPortal).
type catchUpTestMatrix struct {
	testMatrixConnector
}

func (catchUpTestMatrix) NewUserIntent(context.Context, id.UserID, string) (bridgev2.MatrixAPI, string, error) {
	return nil, "", errors.New("double puppeting isn't available in tests")
}

func (catchUpTestMatrix) BotIntent() bridgev2.MatrixAPI { return catchUpTestBot{} }

type catchUpTestBot struct {
	bridgev2.MatrixAPI
}

func (catchUpTestBot) EnsureInvited(context.Context, id.RoomID, id.UserID) error { return nil }

// newCatchUpTestBridge returns a bridge with the WhatsApp tables and a paired login whose client instance is current.
// The database is a SQLite file in WAL mode with a busy timeout, because the portal loop, the flag waiter and the
// test use it at the same time.
func newCatchUpTestBridge(t *testing.T) (*bridgev2.Bridge, *bridgev2.UserLogin) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dsn := "file:" + filepath.Join(t.TempDir(), "bridge.db") + "?_busy_timeout=10000&_journal_mode=WAL&_foreign_keys=on"
	db, err := dbutil.NewWithDialect(dsn, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	wa := &WhatsAppConnector{}
	br := bridgev2.NewBridge("test", db, zerolog.Nop(), nil, catchUpTestMatrix{}, testNetworkConnector{wa},
		func(*bridgev2.Bridge) bridgev2.CommandProcessor { return nil })
	br.BackgroundCtx = ctx
	if err = br.DB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	wa.DB = wadb.New(br.ID, br.DB.Database, zerolog.Nop())
	if err = wa.DB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	wa.Config.HistorySync.MaxInitialConversations = -1
	user, err := br.GetUserByMXID(ctx, "@user:example.com")
	if err != nil {
		t.Fatal(err)
	}
	ul, _ := pairLogin(t, user, testPhoneA)
	return br, ul
}

// markRepairWithinEpoch sets the metadata of a re-pair within intbox.reimport_after (the import epoch started an
// hour before this pairing) whose history sync portals still need creating, and saves it.
func markRepairWithinEpoch(t *testing.T, ul *bridgev2.UserLogin) (loginTS, epoch jsontime.Unix) {
	t.Helper()
	now := time.Now().Truncate(time.Second)
	loginTS, epoch = jsontime.U(now), jsontime.U(now.Add(-time.Hour))
	meta := ul.Metadata.(*waid.UserLoginMetadata)
	meta.LoggedInAt = loginTS
	meta.ImportEpoch = epoch
	meta.HistorySyncPortalsNeedCreating = true
	if err := ul.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !meta.ImportEpochCarried() {
		t.Fatal("test setup: the import epoch should be carried")
	}
	return
}

// savedPortalsFlag reads HistorySyncPortalsNeedCreating as saved in the database (what a restart would see).
func savedPortalsFlag(t *testing.T, br *bridgev2.Bridge, ul *bridgev2.UserLogin) bool {
	t.Helper()
	saved, err := br.DB.UserLogin.GetByID(context.Background(), ul.ID)
	if err != nil || saved == nil {
		t.Fatalf("failed to read user login: %v", err)
	}
	return saved.Metadata.(*waid.UserLoginMetadata).HistorySyncPortalsNeedCreating
}

func countSyncedSince(t *testing.T, ul *bridgev2.UserLogin, ts jsontime.Unix) int {
	t.Helper()
	count, err := ul.Client.(*WhatsAppClient).Main.DB.Conversation.CountSynced(context.Background(), ul.ID, ts)
	if err != nil {
		t.Fatalf("failed to count synced conversations: %v", err)
	}
	return count
}

func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A re-pair whose history sync only has chats that already have rooms: the import round has no rooms, so it's
// done at once, but the catch-up resyncs are still in portal queues. The flag must stay set until they're handled,
// otherwise a restart in between would never queue them again and the messages from the unlinked period would be
// lost for those chats.
func TestRepairCatchUpsKeepPortalsFlagUntilHandled(t *testing.T) {
	br, ul := newCatchUpTestBridge(t)
	ctx := context.Background()
	client := ul.Client.(*WhatsAppClient)
	loginTS, epoch := markRepairWithinEpoch(t, ul)

	// A group imported in the first pairing of the epoch, whose room still exists, with a message from the time
	// when no device was linked (only the new device's history sync has it).
	chat := types.NewJID("120363000000000001", types.GroupServer)
	if err := client.Main.DB.Conversation.Put(ctx, &wadb.Conversation{
		UserLoginID: ul.ID, ChatJID: chat, LastMessageTimestamp: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := client.Main.DB.Conversation.MarkSynced(ctx, ul.ID, chat, epoch); err != nil {
		t.Fatal(err)
	}
	portalKey := client.makeWAPortalKey(chat)
	portal, err := br.GetPortalByKey(ctx, portalKey)
	if err != nil {
		t.Fatal(err)
	}
	// The login is in the room (hasPortalRoomForLogin)
	if _, err = br.DB.UserPortal.GetOrCreate(ctx, ul.UserLogin, portalKey); err != nil {
		t.Fatalf("failed to create user portal: %v", err)
	}
	portal.MXID = "!catchup:example.com"
	if err = portal.Save(ctx); err != nil {
		t.Fatal(err)
	}

	// Hold the portal's event loop, so that the catch-up stays queued until the test releases it.
	entered, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	res := ul.QueueRemoteEvent(&simplevent.ChatResync{EventMeta: simplevent.EventMeta{
		Type:      bridgev2.RemoteEventChatResync,
		PortalKey: portalKey,
		PostHandleFunc: func(context.Context, *bridgev2.Portal) {
			close(entered)
			<-release
		},
	}})
	if !res.Queued {
		t.Fatalf("blocking event wasn't queued: %+v", res)
	}
	waitClosed(t, entered, "the blocking event to be handled")

	client.createPortalsFromHistorySync(ctx)

	for range 15 {
		if !savedPortalsFlag(t, br, ul) {
			t.Fatal("HistorySyncPortalsNeedCreating was cleared while a catch-up was still queued")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := countSyncedSince(t, ul, loginTS); n != 0 {
		t.Fatalf("the catch-up can't have been handled yet, but %d conversations are synced in this pairing", n)
	}

	releaseOnce()
	waitForCondition(t, "the catch-up to mark the conversation as synced", func() bool {
		return countSyncedSince(t, ul, loginTS) == 1
	})
	waitForCondition(t, "HistorySyncPortalsNeedCreating to be cleared after the catch-up", func() bool {
		return !savedPortalsFlag(t, br, ul)
	})
}

// Nothing to import and nothing to catch up: the flag is cleared right away, like before catch-ups existed.
func TestRepairWithoutCandidatesClearsPortalsFlag(t *testing.T) {
	br, ul := newCatchUpTestBridge(t)
	markRepairWithinEpoch(t, ul)
	ul.Client.(*WhatsAppClient).createPortalsFromHistorySync(context.Background())
	waitForCondition(t, "HistorySyncPortalsNeedCreating to be cleared", func() bool {
		return !savedPortalsFlag(t, br, ul)
	})
}

func TestPendingResyncs(t *testing.T) {
	isClosed := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	if !isClosed(newPendingResyncs().seal()) {
		t.Fatal("nothing pending: the channel should be closed")
	}

	pr := newPendingResyncs()
	first, second := pr.add(), pr.add()
	first()
	first() // calls after the first do nothing
	done := pr.seal()
	if isClosed(done) {
		t.Fatal("closed while an event is still pending")
	}
	second()
	if !isClosed(done) {
		t.Fatal("not closed after every event was handled")
	}
	second()

	pr = newPendingResyncs()
	handled := pr.add()
	handled()
	if !isClosed(pr.seal()) {
		t.Fatal("every event was handled before seal: the channel should be closed")
	}
}
