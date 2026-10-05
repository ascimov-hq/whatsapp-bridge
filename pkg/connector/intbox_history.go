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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/wadb"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// L1: history bypass.
//
// When enabled, the initial forward backfill of a chat only sends the newest matrix_recent_messages
// messages to Matrix. The older history sync messages of the chat are moved from the history sync buffer
// into the whatsapp_intbox_history table and served to the app through the history provisioning API,
// keyed by WhatsApp message ID. Rows are deleted when the app acknowledges them, or after the retention.
//
// Matrix and the bypass together never get more than backfill.max_initial_messages messages of a chat,
// and only messages inside history_sync.max_initial_age: the bypass keeps the newest
// max_initial_messages minus (messages sent to Matrix) older messages, and the rest (oldest first)
// are dropped from the history sync buffer. Only the initial forward backfill moves messages into
// the bypass table, so later backfills of the same chat can't add more.

// intboxMatrixBackfillCount returns the number of messages to fetch for a backfill request.
// Called at the start of FetchMessages.
func (wa *WhatsAppClient) intboxMatrixBackfillCount(params bridgev2.FetchMessagesParams) int {
	cfg := wa.Main.Config.Intbox.HistoryBypass
	if !cfg.Enabled || !params.Forward || params.AnchorMessage != nil ||
		cfg.MatrixRecentMessages <= 0 || params.Count <= cfg.MatrixRecentMessages {
		return params.Count
	}
	return cfg.MatrixRecentMessages
}

// intboxHistoryBypassLimit returns how many older messages of a chat the history bypass may keep after the
// initial forward backfill sent sentCount messages to Matrix, so that both together stay within maxInitialMessages.
func intboxHistoryBypassLimit(maxInitialMessages, sentCount int) int {
	return max(maxInitialMessages-sentCount, 0)
}

// intboxWrapHistoryBypass makes the complete callback of the initial forward backfill of a chat (the only
// forward backfill without an anchor) move the older history into the bypass table. The move runs before the
// original callback, which deletes the backfilled messages (or the whole chat) from the history sync buffer.
// Called in FetchMessages with the oldest timestamp in the response.
func (wa *WhatsAppClient) intboxWrapHistoryBypass(
	ctx context.Context, params bridgev2.FetchMessagesParams, portalJID types.JID, oldestTS uint64, resp *bridgev2.FetchMessagesResponse,
) {
	if !wa.Main.Config.Intbox.HistoryBypass.Enabled || !params.Forward || params.AnchorMessage != nil || oldestTS == 0 {
		return
	}
	sentCount := len(resp.Messages)
	origCallback := resp.CompleteCallback
	resp.CompleteCallback = func() {
		wa.intboxStashHistory(ctx, portalJID, oldestTS, sentCount)
		if origCallback != nil {
			origCallback()
		}
	}
}

// intboxStashHistory moves the history sync messages of a chat that are older than oldestTS and inside
// history_sync.max_initial_age into the bypass table, keeping at most backfill.max_initial_messages minus
// sentCount of the newest ones. The older ones are dropped from the history sync buffer.
func (wa *WhatsAppClient) intboxStashHistory(ctx context.Context, portalJID types.JID, oldestTS uint64, sentCount int) {
	limit := intboxHistoryBypassLimit(wa.Main.Bridge.Config.Backfill.MaxInitialMessages, sentCount)
	var notBefore int64
	if maxAge := wa.Main.Config.HistorySync.MaxInitialAge; maxAge > 0 {
		notBefore = time.Now().Add(-maxAge).Unix()
	}
	moved, err := wa.Main.DB.Intbox.StashHistory(ctx, wa.UserLogin.ID, portalJID, int64(oldestTS), notBefore, limit)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Stringer("portal_jid", portalJID).Msg("Failed to move older history to the bypass table")
	} else if moved > 0 {
		zerolog.Ctx(ctx).Debug().
			Stringer("portal_jid", portalJID).
			Int64("moved_count", moved).
			Int("max_moved_count", limit).
			Int("matrix_count", sentCount).
			Uint64("before_ts", oldestTS).
			Msg("Moved older history sync messages to the bypass table")
	}
}

// intboxAfterBackfill is called when a batch of history sync messages has been sent to Matrix.
// It counts the messages for the onboarding status.
func (wa *WhatsAppClient) intboxAfterBackfill(sentCount int) {
	wa.Main.intbox.admission.addMessages(wa.UserLogin.ID, sentCount)
}

func encodeIntboxHistoryCursor(ts int64, messageID, senderJID string) string {
	return base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "%d|%s|%s", ts, messageID, senderJID))
}

func decodeIntboxHistoryCursor(cursor string) (wadb.HistoryCursor, error) {
	if cursor == "" {
		return wadb.HistoryCursor{Timestamp: -1}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return wadb.HistoryCursor{}, err
	}
	parts := strings.SplitN(string(raw), "|", 3)
	if len(parts) != 3 {
		return wadb.HistoryCursor{}, fmt.Errorf("invalid cursor")
	}
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return wadb.HistoryCursor{}, err
	}
	return wadb.HistoryCursor{Timestamp: ts, MessageID: parts[1], SenderJID: parts[2]}, nil
}

type IntboxHistoryRoom struct {
	RoomID  id.RoomID `json:"room_id"`
	ChatID  string    `json:"chat_id"`
	Pending int       `json:"pending"`
}

type IntboxHistoryRoomsResponse struct {
	Rooms []*IntboxHistoryRoom `json:"rooms"`
}

type IntboxHistorySender struct {
	JID       string    `json:"jid"`
	LID       string    `json:"lid"`
	Phone     string    `json:"phone"`
	Name      string    `json:"name"`
	GhostMXID id.UserID `json:"ghost_mxid"`
}

type IntboxHistoryMessage struct {
	WAMessageID        string              `json:"wa_message_id"`
	Sender             IntboxHistorySender `json:"sender"`
	TimestampMS        int64               `json:"timestamp_ms"`
	Type               string              `json:"type"`
	Body               string              `json:"body"`
	FormattedBody      string              `json:"formatted_body"`
	ReplyToWAMessageID string              `json:"reply_to_wa_message_id"`
	Caption            string              `json:"caption"`
	Mimetype           string              `json:"mimetype"`
	Filename           string              `json:"filename"`
}

type IntboxHistoryMessagesResponse struct {
	RoomID   id.RoomID               `json:"room_id"`
	ChatID   string                  `json:"chat_id"`
	Messages []*IntboxHistoryMessage `json:"messages"`
	Next     string                  `json:"next"`
	Done     bool                    `json:"done"`
}

type ReqIntboxHistoryAck struct {
	RoomID id.RoomID `json:"room_id"`
	UpTo   string    `json:"up_to"`
}

type IntboxHistoryAckResponse struct {
	Deleted int64 `json:"deleted"`
}

func intboxLoginIDs(logins []*bridgev2.UserLogin) []networkid.UserLoginID {
	ids := make([]networkid.UserLoginID, len(logins))
	for i, login := range logins {
		ids[i] = login.ID
	}
	return ids
}

// getIntboxHistory implements GET /v3/intbox/history (contract C-2.3).
func (wa *WhatsAppConnector) getIntboxHistory(w http.ResponseWriter, r *http.Request, user *bridgev2.User) {
	log := zerolog.Ctx(r.Context())
	logins := user.GetUserLogins()
	pending, err := wa.DB.Intbox.CountPendingHistory(r.Context(), intboxLoginIDs(logins))
	if err != nil {
		log.Err(err).Msg("Failed to count pending bypass history")
		mautrix.MUnknown.WithMessage("Failed to get pending history").Write(w)
		return
	}
	loginsByID := make(map[networkid.UserLoginID]*bridgev2.UserLogin, len(logins))
	for _, login := range logins {
		loginsByID[login.ID] = login
	}
	resp := &IntboxHistoryRoomsResponse{Rooms: make([]*IntboxHistoryRoom, 0, len(pending))}
	roomsByID := make(map[id.RoomID]*IntboxHistoryRoom, len(pending))
	for _, item := range pending {
		login := loginsByID[item.LoginID]
		if login == nil {
			continue
		}
		client, ok := login.Client.(*WhatsAppClient)
		if !ok {
			continue
		}
		chatJID, err := types.ParseJID(item.ChatJID)
		if err != nil {
			continue
		}
		portal, err := wa.Bridge.GetExistingPortalByKey(r.Context(), client.makeWAPortalKey(chatJID))
		if err != nil {
			log.Err(err).Str("chat_jid", item.ChatJID).Msg("Failed to get portal for pending bypass history")
			continue
		} else if portal == nil || portal.MXID == "" {
			continue
		}
		if room, ok := roomsByID[portal.MXID]; ok {
			room.Pending += item.Count
			continue
		}
		room := &IntboxHistoryRoom{RoomID: portal.MXID, ChatID: item.ChatJID, Pending: item.Count}
		roomsByID[portal.MXID] = room
		resp.Rooms = append(resp.Rooms, room)
	}
	exhttp.WriteJSONResponse(w, http.StatusOK, resp)
}

// getIntboxHistoryMessages implements GET /v3/intbox/history/messages (contract C-2.3).
func (wa *WhatsAppConnector) getIntboxHistoryMessages(w http.ResponseWriter, r *http.Request, user *bridgev2.User) {
	log := zerolog.Ctx(r.Context())
	query := r.URL.Query()
	roomID := id.RoomID(query.Get("room_id"))
	if roomID == "" {
		mautrix.MInvalidParam.WithMessage("room_id is required").Write(w)
		return
	}
	portal, logins := wa.intboxPortalForRequest(w, r, user, roomID, "")
	if portal == nil {
		return
	}
	chatJID, err := waid.ParsePortalID(portal.ID)
	if err != nil {
		mautrix.MInvalidParam.WithMessage("Invalid portal").Write(w)
		return
	}
	after, err := decodeIntboxHistoryCursor(query.Get("after"))
	if err != nil {
		mautrix.MInvalidParam.WithMessage("Invalid cursor").Write(w)
		return
	}
	limit := intboxPageLimit(r)
	rows, err := wa.DB.Intbox.GetHistoryPage(r.Context(), intboxLoginIDs(logins), chatJID, after, limit+1)
	if err != nil {
		log.Err(err).Msg("Failed to get bypass history page")
		mautrix.MUnknown.WithMessage("Failed to get history").Write(w)
		return
	}
	resp := &IntboxHistoryMessagesResponse{
		RoomID:   portal.MXID,
		ChatID:   chatJID.String(),
		Messages: make([]*IntboxHistoryMessage, 0, min(len(rows), limit)),
		Next:     query.Get("after"),
		Done:     len(rows) <= limit,
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	clients := make(map[networkid.UserLoginID]*WhatsAppClient, len(logins))
	for _, login := range logins {
		if client, ok := login.Client.(*WhatsAppClient); ok && client.Client != nil {
			clients[login.ID] = client
		}
	}
	names := make(map[types.JID]string)
	var prevID string
	for _, row := range rows {
		resp.Next = encodeIntboxHistoryCursor(row.Timestamp, row.MessageID, row.SenderJID)
		if row.MessageID+"|"+row.SenderJID == prevID {
			// Same message stored by multiple logins of this user
			continue
		}
		prevID = row.MessageID + "|" + row.SenderJID
		client := clients[row.LoginID]
		if client == nil {
			for _, anyClient := range clients {
				client = anyClient
				break
			}
		}
		if client == nil {
			mautrix.MUnknown.WithMessage("No connected login to decode history with").Write(w)
			return
		}
		msg, err := client.intboxConvertHistoryRow(r.Context(), chatJID, row, names)
		if err != nil {
			log.Warn().Err(err).Str("message_id", row.MessageID).Msg("Failed to convert bypass history message")
			continue
		} else if msg != nil {
			resp.Messages = append(resp.Messages, msg)
		}
	}
	exhttp.WriteJSONResponse(w, http.StatusOK, resp)
}

// postIntboxHistoryAck implements POST /v3/intbox/history/ack (contract C-2.3).
func (wa *WhatsAppConnector) postIntboxHistoryAck(w http.ResponseWriter, r *http.Request, user *bridgev2.User) {
	var req ReqIntboxHistoryAck
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		mautrix.MBadJSON.WithMessage("Failed to decode request body").Write(w)
		return
	} else if req.RoomID == "" || req.UpTo == "" {
		mautrix.MInvalidParam.WithMessage("room_id and up_to are required").Write(w)
		return
	}
	upTo, err := decodeIntboxHistoryCursor(req.UpTo)
	if err != nil {
		mautrix.MInvalidParam.WithMessage("Invalid cursor").Write(w)
		return
	}
	portal, logins := wa.intboxPortalForRequest(w, r, user, req.RoomID, "")
	if portal == nil {
		return
	}
	chatJID, err := waid.ParsePortalID(portal.ID)
	if err != nil {
		mautrix.MInvalidParam.WithMessage("Invalid portal").Write(w)
		return
	}
	deleted, err := wa.DB.Intbox.AckHistory(r.Context(), intboxLoginIDs(logins), chatJID, upTo)
	if err != nil {
		zerolog.Ctx(r.Context()).Err(err).Msg("Failed to delete acknowledged bypass history")
		mautrix.MUnknown.WithMessage("Failed to acknowledge history").Write(w)
		return
	}
	exhttp.WriteJSONResponse(w, http.StatusOK, &IntboxHistoryAckResponse{Deleted: deleted})
}

func (wa *WhatsAppClient) intboxConvertHistoryRow(
	ctx context.Context, chatJID types.JID, row *wadb.BypassHistoryMessage, names map[types.JID]string,
) (*IntboxHistoryMessage, error) {
	var historySyncMsg waHistorySync.HistorySyncMsg
	err := proto.Unmarshal(row.Data, &historySyncMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	evt, err := wa.Client.ParseWebMessage(chatJID, historySyncMsg.GetMessage())
	if err != nil {
		return nil, fmt.Errorf("failed to parse message: %w", err)
	}
	msg := evt.Message
	if msg.GetAssociatedChildMessage().GetMessage() != nil {
		msg = msg.GetAssociatedChildMessage().GetMessage()
	}
	converted := &IntboxHistoryMessage{
		WAMessageID: evt.Info.ID,
		TimestampMS: evt.Info.Timestamp.UnixMilli(),
		Sender:      wa.intboxHistorySender(ctx, &evt.Info, names),
	}
	if contextInfo := intboxGetContextInfo(msg); contextInfo != nil {
		converted.ReplyToWAMessageID = contextInfo.GetStanzaID()
	}
	fillIntboxHistoryContent(converted, msg)
	return converted, nil
}

func (wa *WhatsAppClient) intboxHistorySender(ctx context.Context, info *types.MessageInfo, names map[types.JID]string) IntboxHistorySender {
	sender := info.Sender.ToNonAD()
	alt := info.SenderAlt.ToNonAD()
	if info.IsFromMe {
		sender = wa.JID.ToNonAD()
		alt = wa.GetStore().GetLID().ToNonAD()
	}
	var pn, lid types.JID
	for _, jid := range []types.JID{sender, alt} {
		switch jid.Server {
		case types.DefaultUserServer:
			pn = jid
		case types.HiddenUserServer:
			lid = jid
		}
	}
	name := info.PushName
	if name == "" && !info.IsFromMe {
		lookup := sender
		if !pn.IsEmpty() {
			lookup = pn
		}
		var ok bool
		if name, ok = names[lookup]; !ok && wa.Device != nil && wa.Device.ID != nil {
			found, err := wa.Main.DB.Intbox.GetContactNames(ctx, *wa.Device.ID, []types.JID{lookup})
			if err != nil {
				zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to get contact name for bypass history sender")
			}
			name = found[lookup]
			names[lookup] = name
		}
	}
	return IntboxHistorySender{
		JID:       sender.String(),
		LID:       jidStringOrEmpty(lid),
		Phone:     intboxPhoneFromJID(pn),
		Name:      name,
		GhostMXID: wa.Main.intboxGhostMXID(sender),
	}
}

// intboxGetContextInfo returns the context info of whichever message type is set in the message.
func intboxGetContextInfo(msg *waE2E.Message) (contextInfo *waE2E.ContextInfo) {
	if msg == nil {
		return nil
	}
	msg.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
			return true
		}
		withContext, ok := value.Message().Interface().(interface {
			GetContextInfo() *waE2E.ContextInfo
		})
		if ok && withContext.GetContextInfo() != nil {
			contextInfo = withContext.GetContextInfo()
			return false
		}
		return true
	})
	return
}

func fillIntboxHistoryContent(converted *IntboxHistoryMessage, msg *waE2E.Message) {
	switch {
	case msg.Conversation != nil:
		converted.Type = "text"
		converted.Body = msg.GetConversation()
	case msg.ExtendedTextMessage != nil:
		converted.Type = "text"
		converted.Body = msg.GetExtendedTextMessage().GetText()
	case msg.ImageMessage != nil:
		converted.Type = "image"
		converted.Caption = msg.GetImageMessage().GetCaption()
		converted.Body = converted.Caption
		converted.Mimetype = msg.GetImageMessage().GetMimetype()
	case msg.VideoMessage != nil:
		converted.Type = "video"
		converted.Caption = msg.GetVideoMessage().GetCaption()
		converted.Body = converted.Caption
		converted.Mimetype = msg.GetVideoMessage().GetMimetype()
	case msg.PtvMessage != nil:
		converted.Type = "video"
		converted.Caption = msg.GetPtvMessage().GetCaption()
		converted.Body = converted.Caption
		converted.Mimetype = msg.GetPtvMessage().GetMimetype()
	case msg.AudioMessage != nil:
		converted.Type = "audio"
		converted.Mimetype = msg.GetAudioMessage().GetMimetype()
	case msg.DocumentMessage != nil:
		converted.Type = "document"
		converted.Caption = msg.GetDocumentMessage().GetCaption()
		converted.Filename = msg.GetDocumentMessage().GetFileName()
		converted.Mimetype = msg.GetDocumentMessage().GetMimetype()
		converted.Body = converted.Caption
		if converted.Body == "" {
			converted.Body = converted.Filename
		}
	case msg.StickerMessage != nil:
		converted.Type = "sticker"
		converted.Mimetype = msg.GetStickerMessage().GetMimetype()
	case msg.LocationMessage != nil:
		loc := msg.GetLocationMessage()
		converted.Type = "location"
		converted.Body = formatIntboxLocation(loc.GetName(), loc.GetAddress(), loc.GetDegreesLatitude(), loc.GetDegreesLongitude())
	case msg.LiveLocationMessage != nil:
		loc := msg.GetLiveLocationMessage()
		converted.Type = "location"
		converted.Caption = loc.GetCaption()
		converted.Body = formatIntboxLocation(loc.GetCaption(), "", loc.GetDegreesLatitude(), loc.GetDegreesLongitude())
	case msg.ContactMessage != nil:
		converted.Type = "contact"
		converted.Body = msg.GetContactMessage().GetDisplayName()
	case msg.ContactsArrayMessage != nil:
		converted.Type = "contact"
		converted.Body = msg.GetContactsArrayMessage().GetDisplayName()
		if converted.Body == "" {
			names := make([]string, 0, len(msg.GetContactsArrayMessage().GetContacts()))
			for _, contact := range msg.GetContactsArrayMessage().GetContacts() {
				names = append(names, contact.GetDisplayName())
			}
			converted.Body = strings.Join(names, ", ")
		}
	case msg.PollCreationMessage != nil, msg.PollCreationMessageV2 != nil, msg.PollCreationMessageV3 != nil:
		poll := msg.GetPollCreationMessage()
		if poll == nil {
			poll = msg.GetPollCreationMessageV2()
		}
		if poll == nil {
			poll = msg.GetPollCreationMessageV3()
		}
		converted.Type = "poll"
		lines := []string{poll.GetName()}
		for _, opt := range poll.GetOptions() {
			lines = append(lines, "- "+opt.GetOptionName())
		}
		converted.Body = strings.Join(lines, "\n")
	default:
		converted.Type = "other"
		converted.Body = getMessageType(msg)
	}
}

func formatIntboxLocation(name, address string, lat, long float64) string {
	geo := fmt.Sprintf("geo:%f,%f", lat, long)
	parts := make([]string, 0, 3)
	for _, part := range []string{name, address, geo} {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return strings.Join(parts, "\n")
}
