package connector

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/util/ptr"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/wadb"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// Sim-style test phone numbers ("1555" + profile + user index), never real numbers.
const (
	testPhoneA = "15550100001"
	testPhoneB = "15550100002"
	testPhoneC = "15550100003"
	testPhoneD = "15550100004"
)

func newTestConnector(t *testing.T) *WhatsAppConnector {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &WhatsAppConnector{Bridge: &bridgev2.Bridge{BackgroundCtx: ctx}}
}

func newTestLogin(phone string, log zerolog.Logger) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{
		UserLogin: &database.UserLogin{
			ID:       networkid.UserLoginID(phone),
			UserMXID: "@user:example.com",
			Metadata: &waid.UserLoginMetadata{},
		},
		Log: log,
	}
}

// loadTestClient calls LoadUserLogin like bridgev2 does on startup and on every (re-)pairing. The login has no
// WhatsApp device yet, so no whatsmeow client is created.
func loadTestClient(t *testing.T, wa *WhatsAppConnector, login *bridgev2.UserLogin) *WhatsAppClient {
	t.Helper()
	if err := wa.LoadUserLogin(context.Background(), login); err != nil {
		t.Fatalf("LoadUserLogin failed: %v", err)
	}
	return login.Client.(*WhatsAppClient)
}

func TestLoadUserLoginRetiresPreviousInstance(t *testing.T) {
	wa := newTestConnector(t)
	login := newTestLogin(testPhoneA, zerolog.Nop())
	first := loadTestClient(t, wa, login)
	if !first.isCurrent() {
		t.Fatal("first instance should be current")
	}
	// Background work started by the first instance (contact resync, ghost sync, history sync waiter)
	stopped := make(chan struct{})
	go func() {
		<-first.lifeContext().Done()
		close(stopped)
	}()

	// Re-pair: bridgev2 reuses the UserLogin and calls LoadUserLogin again
	second := loadTestClient(t, wa, login)
	if first.isCurrent() {
		t.Fatal("replaced instance is still current")
	} else if !second.isCurrent() {
		t.Fatal("new instance isn't current")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("background work of the replaced instance wasn't cancelled")
	}
	if cause := context.Cause(first.lifeCtx); !errors.Is(cause, errClientReplaced) {
		t.Fatalf("unexpected cancel cause %v", cause)
	}
	if second.lifeCtx.Err() != nil {
		t.Fatal("lifetime context of the new instance was cancelled")
	}
	// Retiring again is a no-op
	select {
	case <-first.retire(context.Background(), errClientReplaced, true):
	case <-time.After(time.Second):
		t.Fatal("second retire didn't return immediately")
	}
}

func TestStaleLogoutDoesNotTouchNewDevice(t *testing.T) {
	wa := newTestConnector(t)
	login := newTestLogin(testPhoneB, zerolog.Nop())
	old := loadTestClient(t, wa, login)
	old.JID = types.NewADJID(testPhoneB, 0, 2)
	var oldStates []status.BridgeState
	old.bridgeStateHook = func(state status.BridgeState) { oldStates = append(oldStates, state) }

	// Re-pair with device 3: bridgev2 NewLogin replaces the whole metadata object and calls LoadUserLogin again
	login.Metadata = &waid.UserLoginMetadata{}
	cur := loadTestClient(t, wa, login)
	meta := login.Metadata.(*waid.UserLoginMetadata)
	meta.WADeviceID = 3
	cur.JID = types.NewADJID(testPhoneB, 0, 3)
	var curStates []status.BridgeState
	cur.bridgeStateHook = func(state status.BridgeState) { curStates = append(curStates, state) }

	// The user removes the old duplicate linked device on the phone
	old.handleWALogout(events.ConnectFailureLoggedOut, false)
	// The same logout arriving through the event handler (an event that was already being dispatched)
	old.handleWAEvent(&events.LoggedOut{Reason: events.ConnectFailureLoggedOut})
	if meta.WADeviceID != 3 {
		t.Fatalf("stale logout changed the device ID of the new device to %d", meta.WADeviceID)
	} else if len(oldStates) != 0 || len(curStates) != 0 {
		t.Fatalf("stale logout sent bridge states: old=%v cur=%v", oldStates, curStates)
	} else if !cur.isCurrent() {
		t.Fatal("stale logout retired the current instance")
	}

	// Now the current device is unlinked: the login is logged out as before
	cur.handleWALogout(events.ConnectFailureLoggedOut, false)
	if meta.WADeviceID != 0 {
		t.Fatalf("logout of the current device didn't clear the device ID (%d)", meta.WADeviceID)
	} else if len(curStates) != 1 || curStates[0].StateEvent != status.StateBadCredentials || curStates[0].Error != WALoggedOut {
		t.Fatalf("unexpected bridge states after logout: %+v", curStates)
	} else if cur.lifeCtx.Err() == nil {
		t.Fatal("logout didn't cancel the background work of the logged out instance")
	}
}

func TestRetireStopsBackgroundWork(t *testing.T) {
	wa := newTestConnector(t)
	login := newTestLogin(testPhoneC, zerolog.Nop())
	c := loadTestClient(t, wa, login)
	c.isNewLogin = true
	finished := make(chan struct{})
	go func() {
		// An automatic resync of a new login first waits up to 5 minutes for the push name history sync
		c.resyncContacts(false, true, false)
		close(finished)
	}()
	select {
	case <-finished:
		t.Fatal("contact resync finished before push names were synced")
	case <-time.After(100 * time.Millisecond):
	}
	c.retire(context.Background(), errClientReplaced, false)
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("contact resync didn't stop after the instance was retired")
	}
	if !login.Metadata.(*waid.UserLoginMetadata).LastContactResync.IsZero() {
		t.Fatal("an interrupted contact resync was recorded as finished")
	}
	// These would use the database or the whatsmeow client if they didn't stop on a retired instance
	c.syncGhost(types.NewJID(testPhoneD, types.DefaultUserServer), "test", nil)
	c.syncRemoteProfile(c.lifeContext(), nil)
	c.resyncContacts(false, false, false)
}

func TestContactResyncRunnerCoalesces(t *testing.T) {
	var cr contactResyncRunner
	var lock sync.Mutex
	var calls []contactResyncRequest
	release := make(chan struct{})
	started := make(chan struct{}, 10)
	fn := func(req contactResyncRequest) {
		lock.Lock()
		calls = append(calls, req)
		first := len(calls) == 1
		lock.Unlock()
		started <- struct{}{}
		if first {
			<-release
		}
	}
	done := make(chan bool, 1)
	go func() {
		done <- cr.run(contactResyncRequest{automatic: true}, fn)
	}()
	<-started
	// Three requests while the first resync runs: none is dropped, all are merged into one rerun
	if cr.run(contactResyncRequest{automatic: true}, fn) ||
		cr.run(contactResyncRequest{forceAvatarSync: true}, fn) ||
		cr.run(contactResyncRequest{automatic: true, allContacts: true}, fn) {
		t.Fatal("request ran while another resync was running")
	}
	close(release)
	if !<-done {
		t.Fatal("first request didn't report that it ran")
	}
	lock.Lock()
	if len(calls) != 2 {
		t.Fatalf("expected exactly one rerun, got %d calls", len(calls))
	} else if expected := (contactResyncRequest{forceAvatarSync: true, automatic: false, allContacts: true}); calls[1] != expected {
		t.Fatalf("unexpected merged rerun %+v", calls[1])
	}
	lock.Unlock()
	// Idle again: the next request runs at once
	if !cr.run(contactResyncRequest{}, fn) {
		t.Fatal("request didn't run on an idle runner")
	}
	lock.Lock()
	defer lock.Unlock()
	if len(calls) != 3 {
		t.Fatalf("expected 3 calls, got %d", len(calls))
	}
}

func TestContactResyncCooldown(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name     string
		last     jsontime.Unix
		cooldown time.Duration
		skip     bool
	}{
		{"never resynced", jsontime.Unix{}, 12 * time.Hour, false},
		{"resynced an hour ago", jsontime.U(now.Add(-time.Hour)), 12 * time.Hour, true},
		{"resynced a day ago", jsontime.U(now.Add(-24 * time.Hour)), 12 * time.Hour, false},
		{"cooldown disabled", jsontime.U(now.Add(-time.Minute)), 0, false},
	} {
		if got := contactResyncOnCooldown(tc.last, tc.cooldown, now); got != tc.skip {
			t.Errorf("%s: expected skip=%t, got %t", tc.name, tc.skip, got)
		}
	}
}

func TestAutomaticContactResyncSkippedOnCooldown(t *testing.T) {
	wa := newTestConnector(t)
	wa.Config.Intbox.ContactResyncCooldown = 12 * time.Hour
	login := newTestLogin(testPhoneD, zerolog.Nop())
	c := loadTestClient(t, wa, login)
	login.Metadata.(*waid.UserLoginMetadata).LastContactResync = jsontime.U(time.Now().Add(-time.Hour))
	c.isNewLogin = true
	finished := make(chan struct{})
	go func() {
		// Without the cooldown this would wait for the push name history sync
		c.resyncContacts(false, true, false)
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("automatic contact resync wasn't skipped within the cooldown")
	}
}

func TestGetStoreWarnsOnce(t *testing.T) {
	wa := newTestConnector(t)
	var buf bytes.Buffer
	login := newTestLogin(testPhoneA, zerolog.New(&buf))
	c := loadTestClient(t, wa, login)
	for i := 0; i < 5; i++ {
		c.GetStore()
	}
	warnings := 0
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, "Returning noop device in GetStore") && strings.Contains(line, `"level":"warn"`) {
			warnings++
		}
	}
	if warnings != 1 {
		t.Fatalf("expected 1 warning, got %d:\n%s", warnings, buf.String())
	}
}

func newTestPhoneLogin(t *testing.T, wa *WhatsAppConnector, user *bridgev2.User) *WALogin {
	t.Helper()
	process, err := wa.CreateLogin(context.Background(), user, LoginFlowIDPhone)
	if err != nil {
		t.Fatalf("CreateLogin failed: %v", err)
	}
	return process.(*WALogin)
}

func TestOnePendingCodePerPhone(t *testing.T) {
	wa := newTestConnector(t)
	user := &bridgev2.User{User: &database.User{MXID: "@user:example.com"}, Log: zerolog.Nop()}
	ctx := context.Background()
	older := newTestPhoneLogin(t, wa, user)
	newer := newTestPhoneLogin(t, wa, user)
	// Each process claims the number once WhatsApp issued its code
	older.claimPhone(testPhoneA)
	if err := newer.waitForCompletingLogin(ctx, testPhoneA); err != nil {
		t.Fatalf("waiting for a pending (not completing) process failed: %v", err)
	}
	newer.claimPhone(testPhoneA)
	// The older process's pending wait returns LOGIN_SUPERSEDED (HTTP 410)
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err := older.Wait(waitCtx)
	var respErr bridgev2.RespError
	if !errors.Is(err, bridgev2.ErrLoginSuperseded) || !errors.As(err, &respErr) || respErr.StatusCode != 410 {
		t.Fatalf("expected LOGIN_SUPERSEDED, got %v", err)
	}
	if got := wa.pendingPhoneLogins.get(testPhoneA); got != newer {
		t.Fatal("the newer process doesn't own the phone number after the older one finished")
	}
	// A second wait of the superseded process keeps returning the same error
	if _, err = older.Wait(waitCtx); !errors.Is(err, bridgev2.ErrLoginSuperseded) {
		t.Fatalf("expected LOGIN_SUPERSEDED again, got %v", err)
	}
	// Other numbers are independent
	other := newTestPhoneLogin(t, wa, user)
	other.claimPhone(testPhoneB)
	if newer.isSuperseded() {
		t.Fatal("claim of another number superseded the pending process")
	}
	newer.Cancel()
	if wa.pendingPhoneLogins.get(testPhoneA) != nil {
		t.Fatal("cancel didn't release the phone number")
	} else if wa.pendingPhoneLogins.get(testPhoneB) != other {
		t.Fatal("cancel released another number")
	}
}

func TestCompletingLoginIsNotSuperseded(t *testing.T) {
	wa := newTestConnector(t)
	user := &bridgev2.User{User: &database.User{MXID: "@user:example.com"}, Log: zerolog.Nop()}
	ctx := context.Background()
	older := newTestPhoneLogin(t, wa, user)
	older.claimPhone(testPhoneC)
	// The user typed the code: WhatsApp accepted the pairing, the process waits for the 515 reconnect
	older.handleEvent(&events.PairSuccess{ID: types.NewADJID(testPhoneC, 0, 3)})
	if !older.IsCompleting() {
		t.Fatal("process isn't completing after PairSuccess")
	}
	newer := newTestPhoneLogin(t, wa, user)
	errCh := make(chan error, 1)
	go func() {
		errCh <- newer.waitForCompletingLogin(ctx, testPhoneC)
	}()
	select {
	case err := <-errCh:
		t.Fatalf("newer process didn't wait for the completing one: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	// Even a claim at this point (the pairing raced with the new code request) doesn't cancel it
	if older.supersede() || older.isSuperseded() {
		t.Fatal("completing process was superseded")
	}
	// Neither does a cancel from the provisioning API (a supersede racing with PairSuccess)
	older.Cancel()
	if older.Closed.Load() || wa.pendingPhoneLogins.get(testPhoneC) != older {
		t.Fatal("cancel closed a completing process")
	}
	// The older process completes (no connected login is left in this test, so the newer one may continue)
	older.finish()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("newer process failed after the older one finished: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("newer process didn't continue after the older one finished")
	}
	if older.IsCompleting() {
		t.Fatal("finished process is still completing")
	} else if wa.pendingPhoneLogins.get(testPhoneC) != nil {
		t.Fatal("finished process didn't release the phone number")
	}
	newer.claimPhone(testPhoneC)
	if wa.pendingPhoneLogins.get(testPhoneC) != newer {
		t.Fatal("newer process doesn't own the phone number")
	}
}

func TestShouldRefuseRepair(t *testing.T) {
	for _, tc := range []struct {
		name                                                string
		sameUser, explicitRelogin, current, connected, want bool
		state                                               status.BridgeStateEvent
	}{
		{"connected login of the same user", true, false, true, true, true, status.StateConnected},
		{"explicit re-login", true, true, true, true, false, status.StateConnected},
		{"other Matrix user", false, false, true, true, false, status.StateConnected},
		{"logged out login", true, false, true, false, false, status.StateBadCredentials},
		{"socket down", true, false, true, false, false, status.StateConnected},
		{"transient disconnect", true, false, true, true, false, status.StateTransientDisconnect},
		{"replaced client", true, false, false, true, false, status.StateConnected},
	} {
		if got := shouldRefuseRepair(tc.sameUser, tc.explicitRelogin, tc.current, tc.connected, tc.state); got != tc.want {
			t.Errorf("%s: expected %t, got %t", tc.name, tc.want, got)
		}
	}
}

func TestCarryImportState(t *testing.T) {
	now := time.Now()
	hourAgo := jsontime.U(now.Add(-time.Hour).Truncate(time.Second))
	resynced := jsontime.U(now.Add(-30 * time.Minute).Truncate(time.Second))
	for _, tc := range []struct {
		name          string
		prev          *waid.UserLoginMetadata
		reimportAfter time.Duration
		carried       bool
		wantEpoch     jsontime.Unix
	}{
		{"re-pair within window", &waid.UserLoginMetadata{ImportEpoch: hourAgo, LoggedInAt: resynced, LastContactResync: resynced}, 168 * time.Hour, true, hourAgo},
		{"login from before import epochs", &waid.UserLoginMetadata{LoggedInAt: hourAgo, LastContactResync: resynced}, 168 * time.Hour, true, hourAgo},
		{"window expired", &waid.UserLoginMetadata{ImportEpoch: jsontime.U(now.Add(-200 * time.Hour))}, 168 * time.Hour, false, jsontime.U(now)},
		{"carrying disabled", &waid.UserLoginMetadata{ImportEpoch: hourAgo}, 0, false, jsontime.U(now)},
		{"no previous metadata", nil, 168 * time.Hour, false, jsontime.U(now)},
		{"very old login without timestamps", &waid.UserLoginMetadata{}, 168 * time.Hour, false, jsontime.U(now)},
	} {
		next := &waid.UserLoginMetadata{LoggedInAt: jsontime.U(now), ImportEpoch: jsontime.U(now)}
		if got := carryImportState(tc.prev, next, tc.reimportAfter, now); got != tc.carried {
			t.Errorf("%s: expected carried=%t, got %t", tc.name, tc.carried, got)
			continue
		}
		if !next.ImportEpoch.Equal(tc.wantEpoch.Time) {
			t.Errorf("%s: unexpected import epoch %v, expected %v", tc.name, next.ImportEpoch, tc.wantEpoch)
		}
		if tc.carried != next.ImportEpochCarried() {
			t.Errorf("%s: ImportEpochCarried is %t", tc.name, next.ImportEpochCarried())
		}
		if tc.carried && !next.LastContactResync.Equal(tc.prev.LastContactResync.Time) {
			t.Errorf("%s: last contact resync wasn't carried", tc.name)
		} else if !tc.carried && !next.LastContactResync.IsZero() {
			t.Errorf("%s: last contact resync was carried", tc.name)
		}
	}
}

func TestHistorySyncChatResyncOnReplacedInstance(t *testing.T) {
	wa := newTestConnector(t)
	login := newTestLogin(testPhoneD, zerolog.Nop())
	old := loadTestClient(t, wa, login)
	evt := &historySyncChatResync{wa: old, ChatResync: &simplevent.ChatResync{
		EventMeta:       simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, CreatePortal: true},
		ChatInfo:        &bridgev2.ChatInfo{Name: ptr.Ptr("Group")},
		LatestMessageTS: time.Now(),
	}}
	ctx := context.Background()
	if info, _ := evt.GetChatInfo(ctx, nil); !evt.ShouldCreatePortal() || info == nil {
		t.Fatal("event of the current instance should create the portal with chat info")
	} else if needed, _ := evt.CheckNeedsBackfill(ctx, nil); !needed {
		t.Fatal("event of the current instance should backfill")
	}
	loadTestClient(t, wa, login)
	if info, _ := evt.GetChatInfo(ctx, nil); evt.ShouldCreatePortal() || info != nil {
		t.Fatal("event queued by a replaced instance shouldn't create the portal or apply chat info")
	} else if needed, _ := evt.CheckNeedsBackfill(ctx, nil); needed {
		t.Fatal("event queued by a replaced instance shouldn't backfill")
	}
}

func TestExampleConfigIntboxDefaults(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(ExampleConfig), &cfg); err != nil {
		t.Fatalf("failed to parse example config: %v", err)
	}
	if cfg.Intbox.ReimportAfter != 168*time.Hour {
		t.Errorf("unexpected reimport_after %s", cfg.Intbox.ReimportAfter)
	}
	if cfg.Intbox.ContactResyncCooldown != 12*time.Hour {
		t.Errorf("unexpected contact_resync_cooldown %s", cfg.Intbox.ContactResyncCooldown)
	}
	if !cfg.Intbox.RefuseConnectedRepair || !cfg.Intbox.OneCodePerPhone {
		t.Errorf("refuse_connected_repair and one_code_per_phone should default to true")
	}
}

func TestPartitionImportCandidates(t *testing.T) {
	epoch := jsontime.UInt(10_000)
	cand := func(id string, synced int64) *wadb.ImportCandidate {
		ic := &wadb.ImportCandidate{Conversation: &wadb.Conversation{ChatJID: types.NewJID(id, types.GroupServer)}}
		if synced != 0 {
			ic.SyncedLoginTS = jsontime.UInt(synced)
		}
		return ic
	}
	// Most recent first. a: imported in the epoch, room exists. b: imported in the epoch, room deleted.
	// c: never imported, room exists (e.g. created by a live message). d, e: never imported, no room.
	// f: never imported, portal still being created.
	candidates := []*wadb.ImportCandidate{cand("a", 10_000), cand("b", 10_000), cand("c", 0), cand("d", 0), cand("e", 5_000), cand("f", 0)}
	rooms := map[string]bool{"a": true, "c": true}
	hasRoom := func(jid types.JID) bool { return rooms[jid.User] }
	inFlight := map[types.JID]struct{}{types.NewJID("f", types.GroupServer): {}}
	ids := func(convs []*wadb.Conversation) string {
		out := make([]string, len(convs))
		for i, conv := range convs {
			out[i] = conv.ChatJID.User
		}
		return strings.Join(out, ",")
	}

	// Re-pair within reimport_after
	newRooms, catchUps, imported := partitionImportCandidates(candidates, inFlight, true, epoch, -1, hasRoom)
	if ids(newRooms) != "d,e" || ids(catchUps) != "a,c" || ids(imported) != "b" {
		t.Fatalf("unexpected re-pair partition: new=%s catch-up=%s imported=%s", ids(newRooms), ids(catchUps), ids(imported))
	}
	// The per-login room cap only limits new rooms, never catch-ups
	newRooms, catchUps, _ = partitionImportCandidates(candidates, inFlight, true, epoch, 1, hasRoom)
	if ids(newRooms) != "d" || ids(catchUps) != "a,c" {
		t.Fatalf("unexpected capped re-pair partition: new=%s catch-up=%s", ids(newRooms), ids(catchUps))
	}
	newRooms, catchUps, _ = partitionImportCandidates(candidates, inFlight, true, epoch, 0, hasRoom)
	if len(newRooms) != 0 || ids(catchUps) != "a,c" {
		t.Fatalf("unexpected partition with the cap reached: new=%s catch-up=%s", ids(newRooms), ids(catchUps))
	}
	// First pairing, a pairing after reimport_after, or a login from before import epochs: every candidate is
	// a new room (the room lookup isn't even made), exactly like before
	newRooms, catchUps, imported = partitionImportCandidates(candidates, inFlight, false, epoch, -1, func(types.JID) bool {
		t.Fatal("room lookup without a carried epoch")
		return false
	})
	if ids(newRooms) != "a,b,c,d,e" || len(catchUps) != 0 || len(imported) != 0 {
		t.Fatalf("unexpected partition without a carried epoch: new=%s catch-up=%s imported=%s", ids(newRooms), ids(catchUps), ids(imported))
	}
	newRooms, _, _ = partitionImportCandidates(candidates, nil, false, epoch, 3, hasRoom)
	if ids(newRooms) != "a,b,c" {
		t.Fatalf("unexpected limited partition: %s", ids(newRooms))
	}
}
