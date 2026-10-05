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
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	up "go.mau.fi/util/configupgrade"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// IntboxConfig contains the intbox-specific onboarding capacity options (network.intbox in the bridge config).
// Every option defaults to the upstream behaviour when it's not set.
type IntboxConfig struct {
	Roster struct {
		// Persist group participant lists, serve them through the roster provisioning API
		// and publish the com.zeldin.group_info state event in group portals.
		Enabled bool `yaml:"enabled"`
	} `yaml:"roster"`
	Admission struct {
		// Queue history sync portal creation and backfill so that only a limited number of logins import at once.
		Enabled bool `yaml:"enabled"`
		// Number of logins that may import history at the same time.
		MaxConcurrentImports int `yaml:"max_concurrent_imports"`
		// Admit the next queued login early when fewer than this many rooms are still importing.
		MinRoomsInFlight int `yaml:"min_rooms_in_flight"`
		// Small imports (few rooms and few buffered history messages) have a lane of their own, so a storm of
		// small accounts doesn't queue in front of real onboardings, and doesn't wait behind big imports either.
		// Disabled when max_rooms or max_concurrent is 0.
		SmallImports struct {
			// An import is small if its first history sync round has at most this many rooms...
			MaxRooms int `yaml:"max_rooms"`
			// ...and the login has at most this many buffered history sync messages (0: rooms only).
			MaxMessages int `yaml:"max_messages"`
			// Number of small imports that may run at the same time, on top of max_concurrent_imports.
			MaxConcurrent int `yaml:"max_concurrent"`
		} `yaml:"small_imports"`
		// Heavy imports (many buffered history messages) may only take some of the max_concurrent_imports
		// slots, so that a few very big accounts can't hold every slot for hours. Disabled when either is 0.
		HeavyImports struct {
			// An import is heavy if the login has at least this many buffered history sync messages.
			MinMessages int `yaml:"min_messages"`
			// Number of heavy imports that may run at the same time (they count against max_concurrent_imports).
			MaxConcurrent int `yaml:"max_concurrent"`
		} `yaml:"heavy_imports"`
	} `yaml:"admission"`
	HistoryBypass struct {
		// Only send the newest messages of each chat to Matrix and serve older history-sync messages
		// through the history provisioning API instead. Both together never exceed backfill.max_initial_messages
		// per chat (the oldest are dropped) and stay within history_sync.max_initial_age.
		Enabled bool `yaml:"enabled"`
		// Number of newest messages per chat that are still backfilled into Matrix. The bypass serves at most
		// backfill.max_initial_messages minus the number of messages that went to Matrix.
		MatrixRecentMessages int `yaml:"matrix_recent_messages"`
		// How long to keep bypassed history that the app hasn't acknowledged. 0 keeps it forever.
		Retention time.Duration `yaml:"retention"`
	} `yaml:"history_bypass"`
	// How long to keep rows in the history sync buffer (whatsapp_history_sync_message). 0 keeps them forever.
	HistoryBufferRetention time.Duration `yaml:"history_buffer_retention"`
	// When the same WhatsApp account is paired again less than this long after its history import started,
	// keep the import state (chats already imported, per-login room cap, last contact resync) instead of
	// importing everything again. Chats that already have a room only get a catch-up backfill.
	// 0 imports from scratch on every pairing (upstream behaviour).
	ReimportAfter time.Duration `yaml:"reimport_after"`
	// Minimum time between automatic contact resyncs of a login (the one after every pairing).
	// 0 disables the cooldown (upstream behaviour). The sync contacts command is never skipped.
	ContactResyncCooldown time.Duration `yaml:"contact_resync_cooldown"`
	// Refuse a pairing code request (HTTP 409 FI.MAU.BRIDGE.ALREADY_LOGGED_IN) for a phone number whose login
	// of the same Matrix user is connected right now, unless it's an explicit re-login (?login_id=).
	// Pairing again would only link a duplicate device. false keeps the upstream behaviour.
	RefuseConnectedRepair bool `yaml:"refuse_connected_repair"`
	// Keep only one pending pairing code per phone number: a newer code request cancels the older login process
	// of the same number (its pending step returns HTTP 410 FI.MAU.BRIDGE.LOGIN_SUPERSEDED), unless that process
	// already paired. This is the connector-side counterpart of provisioning.supersede_logins, which works per
	// Matrix user. false keeps the upstream behaviour (several pending codes per number).
	OneCodePerPhone bool `yaml:"one_code_per_phone"`
}

func upgradeIntboxConfig(helper up.Helper) {
	helper.Copy(up.Bool, "intbox", "roster", "enabled")
	helper.Copy(up.Bool, "intbox", "admission", "enabled")
	helper.Copy(up.Int, "intbox", "admission", "max_concurrent_imports")
	helper.Copy(up.Int, "intbox", "admission", "min_rooms_in_flight")
	helper.Copy(up.Int, "intbox", "admission", "small_imports", "max_rooms")
	helper.Copy(up.Int, "intbox", "admission", "small_imports", "max_messages")
	helper.Copy(up.Int, "intbox", "admission", "small_imports", "max_concurrent")
	helper.Copy(up.Int, "intbox", "admission", "heavy_imports", "min_messages")
	helper.Copy(up.Int, "intbox", "admission", "heavy_imports", "max_concurrent")
	helper.Copy(up.Bool, "intbox", "history_bypass", "enabled")
	helper.Copy(up.Int, "intbox", "history_bypass", "matrix_recent_messages")
	helper.Copy(up.Str|up.Int, "intbox", "history_bypass", "retention")
	helper.Copy(up.Str|up.Int, "intbox", "history_buffer_retention")
	helper.Copy(up.Str|up.Int, "intbox", "reimport_after")
	helper.Copy(up.Str|up.Int, "intbox", "contact_resync_cooldown")
	helper.Copy(up.Bool, "intbox", "refuse_connected_repair")
	helper.Copy(up.Bool, "intbox", "one_code_per_phone")
}

// intboxState is the in-memory state of the intbox features. The zero value is ready to use.
type intboxState struct {
	admission intboxAdmission
	// Group JID -> hash of the last roster written to the database
	rosterHashes sync.Map
	// Portal key -> struct{}, portals waiting for room creation to send the group info state event
	groupInfoWaiters sync.Map
}

const (
	intboxCleanupInterval     = 1 * time.Hour
	intboxFirstCleanupDelay   = 5 * time.Minute
	intboxDefaultPageLimit    = 500
	intboxMaxPageLimit        = 1000
	intboxProvisioningPrefix  = "/v3/intbox"
	intboxGroupInfoCreateWait = 30 * time.Minute
)

// startIntbox registers the intbox provisioning endpoints and starts the retention loop.
// It's called from Start after the database has been upgraded.
func (wa *WhatsAppConnector) startIntbox(ctx context.Context) {
	if wa.Bridge.Background {
		return
	}
	log := zerolog.Ctx(ctx).With().Str("component", "intbox").Logger()
	provConnector, ok := wa.Bridge.Matrix.(bridgev2.MatrixConnectorWithProvisioning)
	if !ok || provConnector.GetProvisioning() == nil || provConnector.GetProvisioning().GetRouter() == nil {
		log.Warn().Msg("Provisioning API not available, not registering intbox endpoints")
	} else {
		prov := provConnector.GetProvisioning()
		router := prov.GetRouter()
		router.HandleFunc("GET "+intboxProvisioningPrefix+"/roster", wa.intboxHandler(prov, wa.getIntboxRoster))
		router.HandleFunc("GET "+intboxProvisioningPrefix+"/onboarding", wa.intboxHandler(prov, wa.getIntboxOnboarding))
		router.HandleFunc("GET "+intboxProvisioningPrefix+"/history", wa.intboxHandler(prov, wa.getIntboxHistory))
		router.HandleFunc("GET "+intboxProvisioningPrefix+"/history/messages", wa.intboxHandler(prov, wa.getIntboxHistoryMessages))
		router.HandleFunc("POST "+intboxProvisioningPrefix+"/history/ack", wa.intboxHandler(prov, wa.postIntboxHistoryAck))
		log.Debug().Msg("Registered intbox provisioning endpoints")
	}
	if wa.Config.Intbox.HistoryBypass.Retention > 0 || wa.Config.Intbox.HistoryBufferRetention > 0 {
		go wa.intboxCleanupLoop(log.WithContext(wa.Bridge.BackgroundCtx))
	}
}

type intboxHandlerFunc func(w http.ResponseWriter, r *http.Request, user *bridgev2.User)

func (wa *WhatsAppConnector) intboxHandler(prov bridgev2.IProvisioningAPI, fn intboxHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := prov.GetUser(r)
		if user == nil {
			mautrix.MForbidden.WithMessage("Unknown user").Write(w)
			return
		}
		fn(w, r, user)
	}
}

// intboxCleanupLoop implements W7: it deletes unacknowledged bypass history and old history sync
// buffer rows once they're older than the configured retention.
func (wa *WhatsAppConnector) intboxCleanupLoop(ctx context.Context) {
	log := zerolog.Ctx(ctx).With().Str("action", "intbox retention cleanup").Logger()
	ctx = log.WithContext(ctx)
	timer := time.NewTimer(intboxFirstCleanupDelay)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
		case <-ctx.Done():
			return
		}
		if retention := wa.Config.Intbox.HistoryBypass.Retention; retention > 0 {
			deleted, err := wa.DB.Intbox.DeleteExpiredHistory(ctx, time.Now().Add(-retention))
			if err != nil {
				log.Err(err).Msg("Failed to delete expired bypass history")
			} else if deleted > 0 {
				log.Info().Int64("deleted_count", deleted).Dur("retention", retention).Msg("Deleted expired bypass history")
			}
		}
		if retention := wa.Config.Intbox.HistoryBufferRetention; retention > 0 {
			deleted, err := wa.DB.Intbox.DeleteExpiredHistorySyncMessages(ctx, time.Now().Add(-retention))
			if err != nil {
				log.Err(err).Msg("Failed to delete expired history sync buffer rows")
			} else if deleted > 0 {
				log.Info().Int64("deleted_count", deleted).Dur("retention", retention).Msg("Deleted expired history sync buffer rows")
			}
		}
		timer.Reset(intboxCleanupInterval)
	}
}

// intboxGhostMXID returns the Matrix user ID of the ghost the bridge uses (or would use) for the given JID.
func (wa *WhatsAppConnector) intboxGhostMXID(jid types.JID) id.UserID {
	if jid.IsEmpty() {
		return ""
	}
	formatter, ok := wa.Bridge.Matrix.(interface {
		FormatGhostMXID(userID networkid.UserID) id.UserID
	})
	if !ok {
		return ""
	}
	return formatter.FormatGhostMXID(waid.MakeUserID(jid))
}

// intboxIdentifiers returns the contact identifiers (as in com.zeldin.bridge_ids) for a phone number and LID.
func intboxIdentifiers(phone string, lid types.JID) []string {
	identifiers := make([]string, 0, 2)
	if phone != "" {
		identifiers = append(identifiers, "tel:"+phone)
	}
	if !lid.IsEmpty() {
		identifiers = append(identifiers, "lid:"+lid.User)
	}
	return identifiers
}

// intboxResolvePortal finds the portal for a room_id or chat_id query/body parameter.
func (wa *WhatsAppConnector) intboxResolvePortal(ctx context.Context, user *bridgev2.User, roomID id.RoomID, chatID string) (*bridgev2.Portal, error) {
	if roomID != "" {
		return wa.Bridge.GetPortalByMXID(ctx, roomID)
	}
	chatJID, err := types.ParseJID(chatID)
	if err != nil || chatJID.IsEmpty() {
		return nil, mautrix.MInvalidParam.WithMessage("Invalid chat_id")
	}
	for _, login := range user.GetUserLogins() {
		client, ok := login.Client.(*WhatsAppClient)
		if !ok {
			continue
		}
		portal, err := wa.Bridge.GetExistingPortalByKey(ctx, client.makeWAPortalKey(chatJID))
		if err != nil {
			return nil, err
		} else if portal != nil {
			return portal, nil
		}
	}
	return nil, nil
}

// intboxUserLoginsInPortal returns the caller's logins that are in the given portal.
func (wa *WhatsAppConnector) intboxUserLoginsInPortal(ctx context.Context, user *bridgev2.User, portal *bridgev2.Portal) ([]*bridgev2.UserLogin, error) {
	userPortals, err := wa.Bridge.DB.UserPortal.GetAllForUserInPortal(ctx, user.MXID, portal.PortalKey)
	if err != nil {
		return nil, err
	}
	logins := user.GetUserLogins()
	inPortal := make([]*bridgev2.UserLogin, 0, len(userPortals))
	for _, up := range userPortals {
		idx := slices.IndexFunc(logins, func(login *bridgev2.UserLogin) bool {
			return login.ID == up.LoginID
		})
		if idx >= 0 {
			inPortal = append(inPortal, logins[idx])
		}
	}
	return inPortal, nil
}

// intboxPortalForRequest resolves the portal of a request and checks that the caller has a login in it.
// It writes the error response and returns nil if the portal can't be used.
func (wa *WhatsAppConnector) intboxPortalForRequest(
	w http.ResponseWriter, r *http.Request, user *bridgev2.User, roomID id.RoomID, chatID string,
) (*bridgev2.Portal, []*bridgev2.UserLogin) {
	log := zerolog.Ctx(r.Context())
	if roomID == "" && chatID == "" {
		mautrix.MInvalidParam.WithMessage("room_id or chat_id is required").Write(w)
		return nil, nil
	}
	portal, err := wa.intboxResolvePortal(r.Context(), user, roomID, chatID)
	if err != nil {
		var respErr mautrix.RespError
		if errors.As(err, &respErr) {
			respErr.Write(w)
		} else {
			log.Err(err).Msg("Failed to get portal for intbox request")
			mautrix.MUnknown.WithMessage("Failed to get portal").Write(w)
		}
		return nil, nil
	} else if portal == nil {
		mautrix.MNotFound.WithMessage("Portal not found").Write(w)
		return nil, nil
	}
	logins, err := wa.intboxUserLoginsInPortal(r.Context(), user, portal)
	if err != nil {
		log.Err(err).Msg("Failed to get user portals for intbox request")
		mautrix.MUnknown.WithMessage("Failed to check chat membership").Write(w)
		return nil, nil
	} else if len(logins) == 0 {
		mautrix.MForbidden.WithMessage("You are not in this chat").Write(w)
		return nil, nil
	}
	return portal, logins
}

func intboxPageLimit(r *http.Request) int {
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		return intboxDefaultPageLimit
	}
	return min(limit, intboxMaxPageLimit)
}
