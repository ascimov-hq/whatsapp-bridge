package connector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// These tests run the real bridgev2 NewLogin (the mautrix-go worktree through go.work, with the upstream 0748d1e
// port that disconnects the replaced client) together with this connector's LoadUserLogin, which retires the
// replaced instance. The whatsmeow side of the old instance is a fake that records the teardown.

var errFakeSocketClosed = errors.New("websocket disconnected before info query returned response")

// fakeRetireConn models the parts of a whatsmeow client that matter for the teardown: Disconnect fails a pending
// Logout request (whatsmeow's Disconnect clears all response waiters), RemoveEventHandlers waits for running
// event handlers, and Disconnect waits for the socket lock.
type fakeRetireConn struct {
	lock  sync.Mutex
	calls map[string]int

	logoutErr      error
	logoutStarted  chan struct{}
	logoutRelease  chan struct{} // Logout blocks until closed (the logout request is in flight)
	socketClosed   chan struct{} // closed by the first Disconnect
	closeOnce      sync.Once
	disconnectWait time.Duration // e.g. the socket lock held by a dial in progress
	removeHook     func()        // runs in RemoveEventHandlers (a handler that is still running)
	removed        chan struct{}
}

func newFakeRetireConn() *fakeRetireConn {
	return &fakeRetireConn{
		calls:         make(map[string]int),
		logoutStarted: make(chan struct{}, 1),
		socketClosed:  make(chan struct{}),
		removed:       make(chan struct{}),
	}
}

func (f *fakeRetireConn) record(call string) {
	f.lock.Lock()
	f.calls[call]++
	f.lock.Unlock()
}

func (f *fakeRetireConn) count(call string) int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.calls[call]
}

func (f *fakeRetireConn) Logout(ctx context.Context) error {
	f.record("logout")
	f.logoutStarted <- struct{}{}
	if f.logoutRelease != nil {
		select {
		case <-f.logoutRelease:
		case <-f.socketClosed:
			f.record("logout aborted")
			return errFakeSocketClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.logoutErr != nil {
		return f.logoutErr
	}
	// A successful whatsmeow Logout disconnects by itself
	f.closeOnce.Do(func() { close(f.socketClosed) })
	return nil
}

func (f *fakeRetireConn) Disconnect() {
	time.Sleep(f.disconnectWait)
	f.record("disconnect")
	f.closeOnce.Do(func() { close(f.socketClosed) })
}

func (f *fakeRetireConn) RemoveEventHandlers() {
	if f.removeHook != nil {
		f.removeHook()
	}
	f.record("remove handlers")
	close(f.removed)
}

type testMatrixConnector struct {
	bridgev2.MatrixConnector
}

func (testMatrixConnector) Init(*bridgev2.Bridge)         {}
func (testMatrixConnector) BotIntent() bridgev2.MatrixAPI { return nil }

// testNetworkConnector is the real connector, without the Init that sets up the message converter and stores.
type testNetworkConnector struct {
	*WhatsAppConnector
}

func (tn testNetworkConnector) Init(br *bridgev2.Bridge) {
	tn.Bridge = br
}

func newRepairTestBridge(t *testing.T) (*bridgev2.Bridge, *bridgev2.User) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	db, err := dbutil.NewWithDialect("file:"+t.Name()+"?mode=memory&cache=shared", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	br := bridgev2.NewBridge("test", db, zerolog.Nop(), nil, testMatrixConnector{}, testNetworkConnector{&WhatsAppConnector{}},
		func(*bridgev2.Bridge) bridgev2.CommandProcessor { return nil })
	br.BackgroundCtx = ctx
	if err = br.DB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	user, err := br.GetUserByMXID(ctx, "@user:example.com")
	if err != nil {
		t.Fatal(err)
	}
	return br, user
}

// pairLogin does what WALogin.completeLogin does after a pairing: NewLogin with the network's LoadUserLogin.
// The login has no device ID here, so LoadUserLogin doesn't need the whatsmeow store.
func pairLogin(t *testing.T, user *bridgev2.User, phone string) (*bridgev2.UserLogin, time.Duration) {
	t.Helper()
	start := time.Now()
	ul, err := user.NewLogin(context.Background(), &database.UserLogin{
		ID:       waid.MakeUserLoginID(types.NewJID(phone, types.DefaultUserServer)),
		Metadata: &waid.UserLoginMetadata{},
	}, &bridgev2.NewLoginParams{DeleteOnConflict: true})
	if err != nil {
		t.Fatalf("NewLogin failed: %v", err)
	}
	return ul, time.Since(start)
}

// installFakeConn gives the current client of the login a companion device and a fake whatsmeow connection.
func installFakeConn(ul *bridgev2.UserLogin, phone string, device uint8, conn *fakeRetireConn) *WhatsAppClient {
	wa := ul.Client.(*WhatsAppClient)
	wa.JID = types.NewADJID(phone, 0, device)
	wa.retireConnHook = conn
	return wa
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// A re-pair with a new companion device whose remote logout of the old device takes longer than
// WALogin.retireReplacedDevice waits: NewLogin's disconnect of the replaced client must neither abort the
// logout nor wait for it while holding the bridge-wide cache lock.
func TestRepairDoesNotAbortSlowRemoteLogout(t *testing.T) {
	br, user := newRepairTestBridge(t)
	ul, _ := pairLogin(t, user, testPhoneA)
	conn := newFakeRetireConn()
	conn.logoutRelease = make(chan struct{})
	old := installFakeConn(ul, testPhoneA, 2, conn)

	// WALogin.completeLogin: retireReplacedDevice starts the remote logout and gives up waiting after 5 seconds
	retired := old.retire(context.Background(), errClientReplacedByPair, true)
	waitClosed(t, conn.logoutStarted, "the remote logout to start")

	reused, took := pairLogin(t, user, testPhoneA)
	if reused != ul {
		t.Fatal("NewLogin didn't reuse the existing login")
	} else if reused.Client == bridgev2.NetworkAPI(old) {
		t.Fatal("NewLogin didn't install a new client")
	} else if took > time.Second {
		t.Fatalf("NewLogin waited %s for the replaced client (bridgev2 gives up after 5s)", took)
	}
	if n := conn.count("disconnect"); n != 0 {
		t.Fatalf("the replaced client was disconnected %d times during its remote logout", n)
	} else if conn.count("logout aborted") != 0 {
		t.Fatal("the remote logout was aborted")
	}
	// The cache lock is free while the logout is still in flight
	if br.GetCachedUserLoginByID(ul.ID) != ul {
		t.Fatal("login isn't cached")
	}

	close(conn.logoutRelease)
	waitClosed(t, retired, "the teardown of the replaced client")
	waitClosed(t, conn.removed, "the event handlers to be removed")
	if conn.count("logout") != 1 || conn.count("logout aborted") != 0 || conn.count("disconnect") != 0 {
		t.Fatalf("unexpected teardown: %v", conn.calls)
	}
	// Later disconnects of the replaced instance are still no-ops
	old.Disconnect()
	if conn.count("disconnect") != 0 {
		t.Fatal("disconnect after the teardown reached whatsmeow")
	}
	if !ul.Client.(*WhatsAppClient).isCurrent() || old.isCurrent() {
		t.Fatal("wrong current instance after the re-pair")
	}
}

// The old device was already unlinked, so its remote logout fails: the socket is closed exactly once.
func TestRepairFailedRemoteLogoutDisconnectsOnce(t *testing.T) {
	_, user := newRepairTestBridge(t)
	ul, _ := pairLogin(t, user, testPhoneB)
	conn := newFakeRetireConn()
	conn.logoutRelease = make(chan struct{})
	conn.logoutErr = errors.New("device already removed")
	old := installFakeConn(ul, testPhoneB, 2, conn)

	retired := old.retire(context.Background(), errClientReplacedByPair, true)
	waitClosed(t, conn.logoutStarted, "the remote logout to start")
	pairLogin(t, user, testPhoneB)
	close(conn.logoutRelease)
	waitClosed(t, retired, "the teardown of the replaced client")
	waitClosed(t, conn.removed, "the event handlers to be removed")
	if conn.count("disconnect") != 1 || conn.count("logout") != 1 || conn.count("remove handlers") != 1 {
		t.Fatalf("unexpected teardown: %v", conn.calls)
	}
}

// LoadUserLogin retires the old instance without a remote logout (same device, or recreateClient). An event
// handler of the old instance is still running and needs the bridge-wide cache lock, which NewLogin holds.
// NewLogin's disconnect of the replaced client waits for the socket to close (like whatsmeow's own Disconnect)
// but not for that handler, and the socket is closed exactly once.
func TestRepairWithoutLogoutDisconnectsOnceWithoutDeadlock(t *testing.T) {
	br, user := newRepairTestBridge(t)
	ul, _ := pairLogin(t, user, testPhoneC)
	conn := newFakeRetireConn()
	conn.disconnectWait = 150 * time.Millisecond
	handlerDone := make(chan struct{})
	conn.removeHook = func() {
		// The running handler looks up the login, which takes the cache lock
		br.GetCachedUserLoginByID(ul.ID)
		close(handlerDone)
	}
	old := installFakeConn(ul, testPhoneC, 2, conn)

	reused, took := pairLogin(t, user, testPhoneC)
	if reused.Client == bridgev2.NetworkAPI(old) {
		t.Fatal("NewLogin didn't install a new client")
	} else if took > 2*time.Second {
		t.Fatalf("NewLogin waited %s for the replaced client: it depends on the old event handlers", took)
	} else if took < conn.disconnectWait {
		t.Fatalf("NewLogin returned after %s, before the socket of the replaced client was closed", took)
	}
	if conn.count("disconnect") != 1 {
		t.Fatalf("the replaced client was disconnected %d times", conn.count("disconnect"))
	}
	waitClosed(t, handlerDone, "the old event handler")
	waitClosed(t, conn.removed, "the event handlers to be removed")
	old.Disconnect()
	if conn.count("disconnect") != 1 || conn.count("logout") != 0 {
		t.Fatalf("unexpected teardown: %v", conn.calls)
	}
}

// The provisioning API finds IsCompleting through the LoginProcess interface: a WhatsApp login process reports
// it from PairSuccess until it finishes, and the provisioning API's cancel doesn't stop it meanwhile.
func TestWALoginIsCompletingThroughProvisioningInterface(t *testing.T) {
	wa := newTestConnector(t)
	user := &bridgev2.User{User: &database.User{MXID: "@user:example.com"}, Log: zerolog.Nop()}
	process, err := wa.CreateLogin(context.Background(), user, LoginFlowIDPhone)
	if err != nil {
		t.Fatal(err)
	}
	completing, ok := process.(bridgev2.LoginProcessCompleting)
	if !ok {
		t.Fatal("WhatsApp login process doesn't implement bridgev2.LoginProcessCompleting")
	} else if completing.IsCompleting() {
		t.Fatal("new process is completing")
	}
	wl := process.(*WALogin)
	wl.handleEvent(&events.PairSuccess{ID: types.NewADJID(testPhoneD, 0, 5)})
	if !completing.IsCompleting() {
		t.Fatal("process isn't completing after PairSuccess")
	}
	process.Cancel()
	if wl.Closed.Load() || !completing.IsCompleting() {
		t.Fatal("cancel stopped a completing process")
	}
	wl.finish()
	if completing.IsCompleting() {
		t.Fatal("finished process is still completing")
	}
}

// The provisioning API asks bridgev2.LoginProcessSuperseded whether the connector already cancelled a process
// (a newer pairing code for the same number) before it treats that process as the user's newest login.
func TestWALoginIsSupersededThroughProvisioningInterface(t *testing.T) {
	wa := newTestConnector(t)
	user := &bridgev2.User{User: &database.User{MXID: "@user:example.com"}, Log: zerolog.Nop()}
	processes := make([]bridgev2.LoginProcess, 2)
	for i := range processes {
		process, err := wa.CreateLogin(context.Background(), user, LoginFlowIDPhone)
		if err != nil {
			t.Fatal(err)
		}
		processes[i] = process
	}
	older, ok := processes[0].(bridgev2.LoginProcessSuperseded)
	if !ok {
		t.Fatal("WhatsApp login process doesn't implement bridgev2.LoginProcessSuperseded")
	}
	newer := processes[1].(bridgev2.LoginProcessSuperseded)
	if older.IsSuperseded() || newer.IsSuperseded() {
		t.Fatal("new process is superseded")
	}
	processes[0].(*WALogin).claimPhone(testPhoneC)
	processes[1].(*WALogin).claimPhone(testPhoneC)
	if !older.IsSuperseded() {
		t.Fatal("process isn't superseded after a newer code for the same number")
	} else if newer.IsSuperseded() {
		t.Fatal("the process with the newest code is superseded")
	}
	// A cancel isn't a supersede: the provisioning API handles its own cancels.
	processes[1].Cancel()
	if newer.IsSuperseded() {
		t.Fatal("cancelled process reports superseded")
	}
}
