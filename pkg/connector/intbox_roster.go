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
	"hash/fnv"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/wadb"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// W2: group roster side table.
//
// With max_member_sync, big groups only have admins and people who have spoken in Matrix. The full
// participant list that the bridge already fetches with GetGroupInfo is persisted in a side table,
// kept up to date from participant change events, and served to the app through the roster
// provisioning endpoint. WhatsApp is never called per roster request.

// IntboxGroupInfoEventType is the state event with the real size of a group (contract C-2.4).
var IntboxGroupInfoEventType = event.Type{Type: "com.zeldin.group_info", Class: event.StateEventType}

type IntboxGroupInfoContent struct {
	TotalMemberCount  int   `json:"total_member_count"`
	SyncedMemberCount int   `json:"synced_member_count"`
	UpdatedAt         int64 `json:"updated_at"`
}

func intboxPhoneFromJID(jid types.JID) string {
	if jid.Server == types.DefaultUserServer && jid.User != "" {
		return "+" + jid.User
	}
	return ""
}

func (wa *WhatsAppClient) intboxSyncsAllMembers(total int) bool {
	return wa.Main.Config.MaxMemberSync < 0 || total < wa.Main.Config.MaxMemberSync
}

func intboxRosterHash(members []*wadb.RosterMember, total, synced int) uint64 {
	// Order-independent hash, WhatsApp doesn't guarantee a stable participant order.
	var sum uint64
	for _, member := range members {
		h := fnv.New64a()
		_, _ = h.Write([]byte(member.JID.String()))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(member.LID.String()))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(member.Phone))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(member.Name))
		var flags byte
		if member.IsAdmin {
			flags |= 1
		}
		if member.IsSuperAdmin {
			flags |= 2
		}
		_, _ = h.Write([]byte{flags})
		sum += h.Sum64()
	}
	return sum ^ (uint64(total) << 32) ^ uint64(synced)
}

// intboxRosterFromGroupInfo persists the participant list of a GetGroupInfo result and makes sure the
// com.zeldin.group_info state event is sent when the portal is updated with this info.
// Called from wrapGroupInfo.
func (wa *WhatsAppClient) intboxRosterFromGroupInfo(ctx context.Context, info *types.GroupInfo, wrapped *bridgev2.ChatInfo) {
	if !wa.Main.Config.Intbox.Roster.Enabled || info == nil || wrapped == nil ||
		info.IsParent || info.JID.Server != types.GroupServer {
		return
	}
	log := zerolog.Ctx(ctx).With().Stringer("group_jid", info.JID).Logger()
	total := len(info.Participants)
	synced := total
	if wrapped.Members != nil && !wrapped.Members.IsFull {
		synced = 0
		for _, member := range wrapped.Members.MemberMap {
			if member.Membership == "" || member.Membership == event.MembershipJoin {
				synced++
			}
		}
	}
	members := make([]*wadb.RosterMember, 0, total)
	nameJIDs := make([]types.JID, 0, total)
	for _, pcp := range info.Participants {
		member := &wadb.RosterMember{
			JID:          pcp.JID.ToNonAD(),
			Name:         pcp.DisplayName,
			IsAdmin:      pcp.IsAdmin || pcp.IsSuperAdmin,
			IsSuperAdmin: pcp.IsSuperAdmin,
		}
		if !pcp.LID.IsEmpty() {
			member.LID = pcp.LID.ToNonAD()
		} else if pcp.JID.Server == types.HiddenUserServer {
			member.LID = member.JID
		}
		// Only use the phone number WhatsApp gave in the group info, never the bridge-wide LID mapping,
		// so the roster doesn't reveal numbers that WhatsApp hides from this user.
		if !pcp.PhoneNumber.IsEmpty() {
			member.Phone = intboxPhoneFromJID(pcp.PhoneNumber.ToNonAD())
			nameJIDs = append(nameJIDs, pcp.PhoneNumber.ToNonAD())
		} else {
			member.Phone = intboxPhoneFromJID(member.JID)
		}
		nameJIDs = append(nameJIDs, member.JID)
		members = append(members, member)
	}
	updater := wa.intboxGroupInfoUpdater(info.JID)
	wrapped.ExtraUpdates = bridgev2.MergeExtraUpdaters(wrapped.ExtraUpdates, updater)

	hash := intboxRosterHash(members, total, synced)
	if prevHash, ok := wa.Main.intbox.rosterHashes.Load(info.JID); ok && prevHash.(uint64) == hash {
		return
	}
	if wa.Device != nil && wa.Device.ID != nil {
		names, err := wa.Main.DB.Intbox.GetContactNames(ctx, *wa.Device.ID, nameJIDs)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to get contact names for group roster")
		}
		for _, member := range members {
			if member.Name != "" {
				continue
			}
			if name, ok := names[member.JID]; ok {
				member.Name = name
			} else if phone := member.Phone; phone != "" {
				member.Name = names[types.NewJID(phone[1:], types.DefaultUserServer)]
			}
		}
	}
	err := wa.Main.DB.Intbox.ReplaceRoster(ctx, info.JID, members, total, synced)
	if err != nil {
		log.Err(err).Msg("Failed to save group roster")
		return
	}
	wa.Main.intbox.rosterHashes.Store(info.JID, hash)
	log.Debug().
		Int("total_member_count", total).
		Int("synced_member_count", synced).
		Msg("Saved group roster")
}

func (wa *WhatsAppClient) intboxRosterMemberFromJID(jid types.JID) *wadb.RosterMember {
	jid = jid.ToNonAD()
	member := &wadb.RosterMember{
		JID:   jid,
		Phone: intboxPhoneFromJID(jid),
	}
	if jid.Server == types.HiddenUserServer {
		member.LID = jid
	}
	return member
}

// intboxRosterFromGroupChange applies participant changes to the stored roster and returns the chat info
// change to apply, with an extra updater that refreshes the com.zeldin.group_info state event if the
// counts changed. Called from wrapGroupInfoChange.
func (wa *WhatsAppClient) intboxRosterFromGroupChange(ctx context.Context, evt *events.GroupInfo, changes *bridgev2.ChatInfo) *bridgev2.ChatInfo {
	if !wa.Main.Config.Intbox.Roster.Enabled || evt == nil ||
		len(evt.Join)+len(evt.Leave)+len(evt.Promote)+len(evt.Demote) == 0 {
		return changes
	}
	log := zerolog.Ctx(ctx).With().Stringer("group_jid", evt.JID).Logger()
	meta, err := wa.Main.DB.Intbox.GetGroupInfo(ctx, evt.JID)
	if err != nil {
		log.Err(err).Msg("Failed to get group info for roster update")
		return changes
	} else if meta == nil {
		// The roster has never been fetched, the next full group info sync will fill it.
		return changes
	}
	// The next full sync must rewrite the roster even if it matches the last full sync.
	wa.Main.intbox.rosterHashes.Delete(evt.JID)
	joined := make([]*wadb.RosterMember, 0, len(evt.Join))
	for _, jid := range evt.Join {
		joined = append(joined, wa.intboxRosterMemberFromJID(jid))
	}
	if err = wa.Main.DB.Intbox.UpsertRosterMembers(ctx, evt.JID, joined); err != nil {
		log.Err(err).Msg("Failed to add joined members to roster")
	}
	for _, jid := range evt.Leave {
		if err = wa.Main.DB.Intbox.DeleteRosterMember(ctx, evt.JID, jid.ToNonAD(), intboxPhoneFromJID(jid.ToNonAD())); err != nil {
			log.Err(err).Stringer("member_jid", jid).Msg("Failed to remove member from roster")
		}
	}
	for _, jid := range evt.Promote {
		if err = wa.Main.DB.Intbox.SetRosterMemberAdmin(ctx, evt.JID, jid.ToNonAD(), intboxPhoneFromJID(jid.ToNonAD()), true); err != nil {
			log.Err(err).Stringer("member_jid", jid).Msg("Failed to mark roster member as admin")
		}
	}
	for _, jid := range evt.Demote {
		if err = wa.Main.DB.Intbox.SetRosterMemberAdmin(ctx, evt.JID, jid.ToNonAD(), intboxPhoneFromJID(jid.ToNonAD()), false); err != nil {
			log.Err(err).Stringer("member_jid", jid).Msg("Failed to mark roster member as non-admin")
		}
	}
	total, eager, err := wa.Main.DB.Intbox.CountRoster(ctx, evt.JID, wa.JID.ToNonAD(), wa.GetStore().GetLID().ToNonAD())
	if err != nil {
		log.Err(err).Msg("Failed to count roster members")
		return changes
	}
	synced := eager
	if wa.intboxSyncsAllMembers(total) {
		synced = total
	}
	if err = wa.Main.DB.Intbox.UpdateGroupCounts(ctx, evt.JID, total, synced); err != nil {
		log.Err(err).Msg("Failed to update group member counts")
		return changes
	}
	if total != meta.SentTotalCount || synced != meta.SentSyncedCount {
		if changes == nil {
			changes = &bridgev2.ChatInfo{}
		}
		changes.ExtraUpdates = bridgev2.MergeExtraUpdaters(changes.ExtraUpdates, wa.intboxGroupInfoUpdater(evt.JID))
	}
	return changes
}

func (wa *WhatsAppClient) intboxGroupInfoUpdater(groupJID types.JID) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		if portal.MXID == "" {
			wa.Main.intboxSendGroupInfoAfterCreate(ctx, portal, groupJID)
		} else {
			wa.Main.intboxSendGroupInfo(ctx, portal, groupJID, false)
		}
		return false
	}
}

// intboxSendGroupInfoAfterCreate sends the group info state event once the portal room has been created.
// Extra updaters run before the room exists when a portal is created, so the event can't be sent inline.
func (wa *WhatsAppConnector) intboxSendGroupInfoAfterCreate(ctx context.Context, portal *bridgev2.Portal, groupJID types.JID) {
	if _, alreadyWaiting := wa.intbox.groupInfoWaiters.LoadOrStore(portal.PortalKey, struct{}{}); alreadyWaiting {
		return
	}
	ctx = zerolog.Ctx(ctx).WithContext(wa.Bridge.BackgroundCtx)
	go func() {
		defer wa.intbox.groupInfoWaiters.Delete(portal.PortalKey)
		if portal.RoomCreated.WaitTimeoutCtx(ctx, intboxGroupInfoCreateWait) != nil {
			return
		}
		wa.intboxSendGroupInfo(ctx, portal, groupJID, true)
	}()
}

// intboxSendGroupInfo sends the com.zeldin.group_info state event if the counts changed since it was last sent.
func (wa *WhatsAppConnector) intboxSendGroupInfo(ctx context.Context, portal *bridgev2.Portal, groupJID types.JID, force bool) {
	log := zerolog.Ctx(ctx)
	meta, err := wa.DB.Intbox.GetGroupInfo(ctx, groupJID)
	if err != nil {
		log.Err(err).Msg("Failed to get group info to send state event")
		return
	} else if meta == nil ||
		(!force && meta.SentTotalCount == meta.TotalMemberCount && meta.SentSyncedCount == meta.SyncedMemberCount) {
		return
	}
	content := &IntboxGroupInfoContent{
		TotalMemberCount:  meta.TotalMemberCount,
		SyncedMemberCount: meta.SyncedMemberCount,
		UpdatedAt:         meta.UpdatedAt,
	}
	// Room metadata goes through the bulk request lane (contract C-1), it must never delay live messages.
	ctx = bridgev2.WithIntboxLane(ctx, mautrix.IntboxLaneBulk, nil)
	_, err = portal.Bridge.Bot.SendState(ctx, portal.MXID, IntboxGroupInfoEventType, "", &event.Content{Parsed: content}, time.Time{})
	if err != nil {
		log.Err(err).Stringer("room_id", portal.MXID).Msg("Failed to send group info state event")
		return
	}
	err = wa.DB.Intbox.MarkGroupInfoSent(ctx, groupJID, meta.TotalMemberCount, meta.SyncedMemberCount)
	if err != nil {
		log.Err(err).Msg("Failed to mark group info state event as sent")
	}
}

type IntboxRosterMember struct {
	JID          string    `json:"jid"`
	LID          string    `json:"lid"`
	Phone        string    `json:"phone"`
	Name         string    `json:"name"`
	IsAdmin      bool      `json:"is_admin"`
	IsSuperAdmin bool      `json:"is_super_admin"`
	GhostMXID    id.UserID `json:"ghost_mxid"`
	Identifiers  []string  `json:"identifiers"`
}

type IntboxRosterResponse struct {
	RoomID            id.RoomID             `json:"room_id"`
	ChatID            string                `json:"chat_id"`
	TotalMemberCount  int                   `json:"total_member_count"`
	SyncedMemberCount int                   `json:"synced_member_count"`
	UpdatedAt         int64                 `json:"updated_at"`
	Members           []*IntboxRosterMember `json:"members"`
	Next              string                `json:"next"`
}

// getIntboxRoster implements GET /v3/intbox/roster (contract C-2.1).
func (wa *WhatsAppConnector) getIntboxRoster(w http.ResponseWriter, r *http.Request, user *bridgev2.User) {
	if !wa.Config.Intbox.Roster.Enabled {
		mautrix.MUnrecognized.WithMessage("The roster is not enabled on this bridge").Write(w)
		return
	}
	log := zerolog.Ctx(r.Context())
	query := r.URL.Query()
	portal, _ := wa.intboxPortalForRequest(w, r, user, id.RoomID(query.Get("room_id")), query.Get("chat_id"))
	if portal == nil {
		return
	}
	groupJID, err := waid.ParsePortalID(portal.ID)
	if err != nil || groupJID.Server != types.GroupServer {
		mautrix.MInvalidParam.WithMessage("The roster is only available for groups").Write(w)
		return
	}
	limit := intboxPageLimit(r)
	meta, err := wa.DB.Intbox.GetGroupInfo(r.Context(), groupJID)
	if err != nil {
		log.Err(err).Msg("Failed to get group info for roster")
		mautrix.MUnknown.WithMessage("Failed to get roster").Write(w)
		return
	}
	members, err := wa.DB.Intbox.GetRoster(r.Context(), groupJID, query.Get("after"), limit+1)
	if err != nil {
		log.Err(err).Msg("Failed to get roster")
		mautrix.MUnknown.WithMessage("Failed to get roster").Write(w)
		return
	}
	resp := &IntboxRosterResponse{
		RoomID:  portal.MXID,
		ChatID:  groupJID.String(),
		Members: make([]*IntboxRosterMember, 0, min(len(members), limit)),
	}
	if meta != nil {
		resp.TotalMemberCount = meta.TotalMemberCount
		resp.SyncedMemberCount = meta.SyncedMemberCount
		resp.UpdatedAt = meta.UpdatedAt
	}
	if len(members) > limit {
		members = members[:limit]
		resp.Next = members[len(members)-1].JID.String()
	}
	for _, member := range members {
		resp.Members = append(resp.Members, &IntboxRosterMember{
			JID:          member.JID.String(),
			LID:          jidStringOrEmpty(member.LID),
			Phone:        member.Phone,
			Name:         member.Name,
			IsAdmin:      member.IsAdmin,
			IsSuperAdmin: member.IsSuperAdmin,
			GhostMXID:    wa.intboxGhostMXID(member.JID),
			Identifiers:  intboxIdentifiers(member.Phone, member.LID),
		})
	}
	exhttp.WriteJSONResponse(w, http.StatusOK, resp)
}

func jidStringOrEmpty(jid types.JID) string {
	if jid.IsEmpty() {
		return ""
	}
	return jid.String()
}
