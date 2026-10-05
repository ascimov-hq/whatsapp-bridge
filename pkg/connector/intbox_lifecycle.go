// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2026 Ascimov Technologies
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// Client instance lifetime (round 2, 4 Oct incident).
//
// bridgev2 reuses the UserLogin object when the same WhatsApp account is paired again, and calls LoadUserLogin
// again, which creates a new WhatsAppClient. The previous instance used to keep running: its socket, its event
// handlers and every goroutine it started (contact resync, ghost sync, the history sync portal waiter) kept
// touching the shared UserLogin, and a logout of the old companion device zeroed the device ID of the new one.
//
// Every instance now owns a lifetime context (lifeCtx) that all of its background work runs on. An instance is
// "current" while it's the UserLogin's client and hasn't been retired. retire() is called when the instance is
// replaced (LoadUserLogin), logged out (LoggedOut event) or explicitly logged out (LogoutRemote). Only the current
// instance may change the login's metadata or bridge state.

var (
	errClientReplaced        = errors.New("client instance was replaced by a newer one for the same login")
	errClientReplacedByPair  = errors.New("login was paired again with a new companion device")
	errClientLoggedOut       = errors.New("client was logged out by WhatsApp")
	errClientLoggedOutBridge = errors.New("client was logged out by the bridge")
)

// retireLogoutTimeout bounds the remote logout of a replaced companion device.
const retireLogoutTimeout = 15 * time.Second

// replacedDeviceLogoutWait is how long a completing pairing waits for the remote logout of the previous
// companion device of the same login before creating the new client.
const replacedDeviceLogoutWait = 5 * time.Second

// completingLoginWait is how long a new pairing code request waits for an older login process of the same
// phone number that already paired (and is only waiting for the post-pair reconnect) to finish.
const completingLoginWait = 20 * time.Second

// lifeContext returns the lifetime context of this client instance. It's cancelled when the instance is retired.
func (wa *WhatsAppClient) lifeContext() context.Context {
	if wa.lifeCtx != nil {
		return wa.lifeCtx
	}
	return wa.Main.Bridge.BackgroundCtx
}

// isActiveInstance returns true if this instance is the client of its UserLogin, even if it has been retired
// because the login was logged out.
func (wa *WhatsAppClient) isActiveInstance() bool {
	return wa.UserLogin != nil && wa.UserLogin.Client == bridgev2.NetworkAPI(wa)
}

// isCurrent returns true if this instance is the client of its UserLogin and hasn't been retired.
// Code that changes the login's metadata or bridge state from a background goroutine or an event handler
// must check this first: a replaced instance shares the UserLogin (and its metadata) with the new one.
func (wa *WhatsAppClient) isCurrent() bool {
	return !wa.retired.Load() && wa.isActiveInstance()
}

// markRetired permanently marks this instance as retired, cancels its lifetime context and stops its loops.
// It returns false if the instance was already retired. It doesn't touch the whatsmeow client.
func (wa *WhatsAppClient) markRetired(cause error) bool {
	if !wa.retired.CompareAndSwap(false, true) {
		return false
	}
	if wa.lifeCancel != nil {
		wa.lifeCancel(cause)
	}
	wa.callStopLoops()
	return true
}

// retireConn is the part of the whatsmeow client that the background teardown of retire uses.
// It's an interface only so that tests can observe the teardown (retireConnHook).
type retireConn interface {
	Logout(ctx context.Context) error
	Disconnect()
	RemoveEventHandlers()
}

var _ retireConn = (*whatsmeow.Client)(nil)

// retireTeardown is the background teardown that retire started for a client instance. From the moment it's
// set, retire owns the socket of the instance: Disconnect doesn't close it a second time (see Disconnect).
type retireTeardown struct {
	// The companion device is logged out remotely before the socket is closed
	logoutRemote bool
	// Closed when the socket is closed (with logoutRemote: after the logout succeeded or failed)
	socketClosed chan struct{}
}

// retireConnection returns the connection that retire tears down, or nil if the instance has none.
func (wa *WhatsAppClient) retireConnection() retireConn {
	if wa.retireConnHook != nil {
		return wa.retireConnHook
	} else if cli := wa.Client; cli != nil {
		return cli
	}
	return nil
}

// retire permanently stops this client instance: its background work is cancelled immediately, then its
// socket is closed and its whatsmeow event handlers are removed. With logoutRemote, the companion device is
// logged out (removed from the phone's linked devices) first, and its local whatsmeow store is deleted even
// if the remote logout fails. Only the first call has an effect.
//
// The whatsmeow part runs in a goroutine, because retire may be called from a whatsmeow event handler, where
// removing event handlers or waiting for an IQ response would deadlock. The returned channel is closed when
// the socket is closed (with logoutRemote: after the remote logout succeeded or failed); the event handlers
// are removed right after that. retire is the only place that closes the socket of a retired instance:
// bridgev2 disconnecting the replaced client after NewLogin (upstream 0748d1e) only waits for it, see Disconnect.
func (wa *WhatsAppClient) retire(ctx context.Context, cause error, logoutRemote bool) <-chan struct{} {
	done := make(chan struct{})
	if !wa.markRetired(cause) {
		close(done)
		return done
	}
	log := wa.UserLogin.Log.With().
		Str("action", "retire client").
		AnErr("cause", cause).
		Bool("logout_remote", logoutRemote).
		Stringer("device_jid", wa.JID).
		Logger()
	conn := wa.retireConnection()
	if conn == nil {
		log.Debug().Msg("Retired client instance without a WhatsApp connection")
		close(done)
		return done
	}
	wa.teardown.Store(&retireTeardown{logoutRemote: logoutRemote, socketClosed: done})
	go func() {
		closeRetiredConnection(ctx, log, conn, logoutRemote)
		close(done)
		// Removing the event handlers waits for handlers that are still running (whatsmeow holds a read lock
		// while it dispatches), e.g. one blocked on the bridge-wide cache lock that NewLogin holds while it
		// replaces this instance. So the socket is closed first and doesn't depend on them. A handler of a
		// retired instance ignores every event anyway (see handleWAEvent).
		conn.RemoveEventHandlers()
		log.Debug().Msg("Retired client instance")
	}()
	return done
}

// closeRetiredConnection closes the socket of a retired client instance. With logoutRemote, the companion device
// is logged out first, and its local whatsmeow store is deleted even if the remote logout fails.
func closeRetiredConnection(ctx context.Context, log zerolog.Logger, conn retireConn, logoutRemote bool) {
	if !logoutRemote {
		conn.Disconnect()
		return
	}
	logoutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), retireLogoutTimeout)
	err := conn.Logout(logoutCtx)
	cancel()
	if err == nil {
		// Logout also disconnects and deletes the local store.
		log.Info().Msg("Logged out replaced companion device")
		return
	}
	log.Info().Err(err).Msg("Failed to log out replaced companion device, it may already be unlinked")
	conn.Disconnect()
	if cli, ok := conn.(*whatsmeow.Client); ok && cli.Store != nil && cli.Store.ID != nil {
		if err = cli.Store.Delete(context.WithoutCancel(ctx)); err != nil {
			log.Warn().Err(err).Msg("Failed to delete whatsmeow store of replaced companion device")
		}
	}
}

// sendBridgeState sends a bridge state for the login. bridgeStateHook is only set by tests.
func (wa *WhatsAppClient) sendBridgeState(state status.BridgeState) {
	if wa.bridgeStateHook != nil {
		wa.bridgeStateHook(state)
		return
	}
	wa.UserLogin.BridgeState.Send(state)
}

// contactResyncRequest contains the parameters of a contact resync.
type contactResyncRequest struct {
	forceAvatarSync bool
	automatic       bool
	allContacts     bool
}

// merge combines two requests into one that does at least as much as both.
func (req contactResyncRequest) merge(other contactResyncRequest) contactResyncRequest {
	return contactResyncRequest{
		forceAvatarSync: req.forceAvatarSync || other.forceAvatarSync,
		automatic:       req.automatic && other.automatic,
		allContacts:     req.allContacts || other.allContacts,
	}
}

// contactResyncRunner makes sure that only one contact resync runs per client instance. A request made while
// a resync is running isn't dropped: all such requests are merged into exactly one rerun, which starts when
// the running resync finishes (the contact store may have changed in between, e.g. a second app state sync).
type contactResyncRunner struct {
	lock    sync.Mutex
	running bool
	pending *contactResyncRequest
}

// run runs fn with req in the calling goroutine, followed by the merged rerun if more requests arrived meanwhile.
// If a resync is already running, the request is merged into the pending rerun and run returns false at once.
func (cr *contactResyncRunner) run(req contactResyncRequest, fn func(contactResyncRequest)) bool {
	cr.lock.Lock()
	if cr.running {
		if cr.pending == nil {
			cr.pending = &req
		} else {
			merged := cr.pending.merge(req)
			cr.pending = &merged
		}
		cr.lock.Unlock()
		return false
	}
	cr.running = true
	cr.lock.Unlock()
	for {
		fn(req)
		cr.lock.Lock()
		if cr.pending == nil {
			cr.running = false
			cr.lock.Unlock()
			return true
		}
		req = *cr.pending
		cr.pending = nil
		cr.lock.Unlock()
	}
}

// contactResyncOnCooldown returns true if an automatic contact resync should be skipped because the last
// one finished less than cooldown ago.
func contactResyncOnCooldown(last jsontime.Unix, cooldown time.Duration, now time.Time) bool {
	return cooldown > 0 && !last.IsZero() && now.Sub(last.Time) < cooldown
}

// carryImportState copies the history import state of the previous pairing of the same account into the
// metadata of a new pairing, if the previous import started less than reimportAfter ago. A re-pair then
// doesn't import (create rooms for, fetch group info of, sync members of) the same chats again, and doesn't
// resync contacts again within the cooldown. Returns true if the state was carried.
func carryImportState(prev, next *waid.UserLoginMetadata, reimportAfter time.Duration, now time.Time) bool {
	if reimportAfter <= 0 || prev == nil || next == nil {
		return false
	}
	epoch := prev.GetImportEpoch()
	if epoch.IsZero() || epoch.After(now) || now.Sub(epoch.Time) >= reimportAfter {
		return false
	}
	next.ImportEpoch = epoch
	next.LastContactResync = prev.LastContactResync
	return true
}

// shouldRefuseRepair decides whether a pairing code request for a phone number must be refused with
// ALREADY_LOGGED_IN: the same Matrix user already has a login for that number that is connected right now,
// and the process wasn't started as an explicit re-login of that login (?login_id=).
func shouldRefuseRepair(sameUser, explicitRelogin, clientCurrent, clientConnected bool, state status.BridgeStateEvent) bool {
	return sameUser && !explicitRelogin && clientCurrent && clientConnected && state == status.StateConnected
}

var nonDigits = regexp.MustCompile("[^0-9]")

// normalizePairingPhone normalizes a phone number the same way whatsmeow's PairPhone does.
func normalizePairingPhone(phone string) string {
	return nonDigits.ReplaceAllString(phone, "")
}

// phoneLoginRegistry tracks the login process that currently has a pending pairing code for each phone number,
// so that a newer pairing code request for the same number cancels the older one (WhatsApp invalidates the
// older code anyway). It's process-wide, because two Matrix users (or two app installs) may pair the same number.
type phoneLoginRegistry struct {
	lock   sync.Mutex
	logins map[string]*WALogin
}

// claim makes wl the pending login process of the phone number and returns the process it replaced, if any.
func (r *phoneLoginRegistry) claim(phone string, wl *WALogin) *WALogin {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.logins == nil {
		r.logins = make(map[string]*WALogin)
	}
	prev := r.logins[phone]
	r.logins[phone] = wl
	if prev == wl {
		return nil
	}
	return prev
}

// release removes wl as the pending login process of the phone number, unless a newer process claimed it.
func (r *phoneLoginRegistry) release(phone string, wl *WALogin) {
	if phone == "" {
		return
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.logins[phone] == wl {
		delete(r.logins, phone)
	}
}

// get returns the pending login process of the phone number.
func (r *phoneLoginRegistry) get(phone string) *WALogin {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.logins[phone]
}

// historySyncChatResync is a chat resync queued by createPortalsFromHistorySync. The portal loop may handle it
// long after it was queued: if the client instance that queued it has been replaced (re-pair) or logged out by
// then, it doesn't create the room, apply chat info or backfill, since the current instance runs its own round.
type historySyncChatResync struct {
	*simplevent.ChatResync
	wa *WhatsAppClient
}

var (
	_ bridgev2.RemoteChatResyncWithInfo       = (*historySyncChatResync)(nil)
	_ bridgev2.RemoteChatResyncBackfill       = (*historySyncChatResync)(nil)
	_ bridgev2.RemoteChatResyncBackfillBundle = (*historySyncChatResync)(nil)
	_ bridgev2.RemoteEventThatMayCreatePortal = (*historySyncChatResync)(nil)
	_ bridgev2.RemotePostHandler              = (*historySyncChatResync)(nil)
)

func (evt *historySyncChatResync) ShouldCreatePortal() bool {
	return evt.ChatResync.ShouldCreatePortal() && evt.wa.isCurrent()
}

func (evt *historySyncChatResync) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	if !evt.wa.isCurrent() {
		return nil, nil
	}
	return evt.ChatResync.GetChatInfo(ctx, portal)
}

func (evt *historySyncChatResync) CheckNeedsBackfill(ctx context.Context, latestMessage *database.Message) (bool, error) {
	if !evt.wa.isCurrent() {
		return false, nil
	}
	return evt.ChatResync.CheckNeedsBackfill(ctx, latestMessage)
}
