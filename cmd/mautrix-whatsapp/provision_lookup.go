package main

import (
	"context"
	"encoding/json"
	"net/http"

	"go.mau.fi/util/exhttp"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/provisionutil"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// ReqContactLookup is the request body for POST /v3/contact-lookup.
type ReqContactLookup struct {
	MXIDs []id.UserID `json:"mxids"`
}

// provContactLookup handles POST /v3/contact-lookup.
//
// Takes a batch of Matrix user IDs (ghost PN, ghost LID, or real bridged user)
// and returns a standard RespResolveIdentifier entry for each resolvable MXID.
// Bot ghosts and unknown MXIDs are silently omitted.
//
// ?login_id= is optional: when present, dm_room_mxid is populated for each
// entry where a DM portal already exists between that login and the contact.
func provContactLookup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// login is optional — only needed for dm_room_mxid.
	var login *bridgev2.UserLogin
	if r.URL.Query().Get("login_id") != "" {
		login = m.Matrix.Provisioning.GetLoginForRequest(w, r)
		if login == nil {
			return // error already written by GetLoginForRequest
		}
	}

	var req ReqContactLookup
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		mautrix.MBadJSON.WithMessage("Failed to decode request: %v", err).Write(w)
		return
	}
	if len(req.MXIDs) > 500 {
		mautrix.MInvalidParam.WithMessage("Too many MXIDs: max 500 per request").Write(w)
		return
	}

	contacts := make([]*provisionutil.RespResolveIdentifier, 0, len(req.MXIDs))
	for _, mxid := range req.MXIDs {
		if entry := resolveContactMXID(ctx, mxid, login); entry != nil {
			contacts = append(contacts, entry)
		}
	}

	exhttp.WriteJSONResponse(w, http.StatusOK, &provisionutil.RespGetContactList{
		Contacts: contacts,
	})
}

// resolveContactMXID resolves a single Matrix user ID to a contact entry.
// Returns nil if the MXID cannot be resolved or should be excluded (e.g. bot).
func resolveContactMXID(ctx context.Context, mxid id.UserID, login *bridgev2.UserLogin) *provisionutil.RespResolveIdentifier {
	networkID, isGhost := m.Matrix.ParseGhostMXID(mxid)

	if isGhost {
		return resolveGhostMXID(ctx, networkID, login)
	}
	return resolveRealUserMXID(ctx, mxid, login)
}

func resolveGhostMXID(ctx context.Context, networkID networkid.UserID, login *bridgev2.UserLogin) *provisionutil.RespResolveIdentifier {
	ghost, err := m.Bridge.GetExistingGhostByID(ctx, networkID)
	if err != nil || ghost == nil {
		return nil
	}
	jid := waid.ParseUserID(networkID)
	if jid.Server == types.BotServer {
		return nil // caller specified "not bots"
	}

	entry := &provisionutil.RespResolveIdentifier{
		ID:          networkID,
		Name:        ghost.Name,
		AvatarURL:   ghost.AvatarMXC,
		Identifiers: ghost.Identifiers,
		MXID:        ghost.Intent.GetMXID(),
	}

	// For LID ghosts, try to resolve to PN for enriched identifiers and DM lookup.
	pnJID := jid
	if jid.Server == types.HiddenUserServer {
		if resolved := resolveLIDToPN(ctx, jid); !resolved.IsEmpty() {
			pnJID = resolved
			if len(entry.Identifiers) == 0 {
				entry.Identifiers = []string{"tel:+" + resolved.User}
			}
		}
	}

	if login != nil && pnJID.Server == types.DefaultUserServer {
		entry.DMRoomID = lookupDMRoom(ctx, login.ID, pnJID)
	}

	return entry
}

func resolveRealUserMXID(ctx context.Context, mxid id.UserID, login *bridgev2.UserLogin) *provisionutil.RespResolveIdentifier {
	logins, err := m.Bridge.DB.UserLogin.GetAllForUser(ctx, mxid)
	if err != nil || len(logins) == 0 {
		return nil // not a bridged user on this bridge
	}

	phoneJID := waid.ParseUserLoginID(logins[0].ID, 0)
	ghostID := waid.MakeUserID(phoneJID)

	entry := &provisionutil.RespResolveIdentifier{
		ID:          ghostID,
		Name:        logins[0].RemoteName,
		Identifiers: []string{"tel:+" + phoneJID.User},
		MXID:        m.Matrix.FormatGhostMXID(ghostID),
	}

	// Override with richer data from the ghost record if it exists.
	if ghost, err := m.Bridge.GetExistingGhostByID(ctx, ghostID); err == nil && ghost != nil {
		if ghost.Name != "" {
			entry.Name = ghost.Name
		}
		entry.AvatarURL = ghost.AvatarMXC
		if len(ghost.Identifiers) > 0 {
			entry.Identifiers = ghost.Identifiers
		}
		entry.MXID = ghost.Intent.GetMXID()
	}

	if login != nil {
		entry.DMRoomID = lookupDMRoom(ctx, login.ID, phoneJID)
	}

	return entry
}

// lookupDMRoom returns the MXID of the existing DM portal between loginID and
// chatJID, or "" if no such portal exists.
func lookupDMRoom(ctx context.Context, loginID networkid.UserLoginID, chatJID types.JID) id.RoomID {
	portalKey := networkid.PortalKey{
		ID:       waid.MakePortalID(chatJID),
		Receiver: loginID,
	}
	portal, err := m.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil || portal == nil {
		return ""
	}
	return portal.MXID
}

// resolveLIDToPN looks up the phone-number JID for a LID by querying the
// whatsmeow LID store of any active WhatsApp client on this bridge.
// Returns types.EmptyJID if the mapping is not found.
func resolveLIDToPN(ctx context.Context, lidJID types.JID) types.JID {
	userIDs, err := m.Bridge.DB.UserLogin.GetAllUserIDsWithLogins(ctx)
	if err != nil {
		return types.EmptyJID
	}
	for _, userMXID := range userIDs {
		user, err := m.Bridge.GetExistingUserByMXID(ctx, userMXID)
		if err != nil || user == nil {
			continue
		}
		for _, ul := range user.GetUserLogins() {
			waClient, ok := ul.Client.(*connector.WhatsAppClient)
			if !ok {
				continue
			}
			pn, err := waClient.GetStore().LIDs.GetPNForLID(ctx, lidJID)
			if err == nil && !pn.IsEmpty() {
				return pn
			}
		}
	}
	return types.EmptyJID
}
