package connector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/util/ptr"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/wadb"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

var _ bridgev2.BackfillingNetworkAPI = (*WhatsAppClient)(nil)

// historySyncResumeDispatchWait is how long a resumed login (bridge restart or reconnect while its history sync
// portals still need creating) waits before dispatching the stored conversations.
var historySyncResumeDispatchWait = 5 * time.Second

// historySyncDispatchDelay is the delay the portal creation timer of historySyncLoop is (re)set to.
//
// intbox (load-test L6-1): upstream armed a 5 s timer for a resumed login, but ManualHistorySyncDownload always wakes
// the loop once at start, and that wakeup stopped the timer and reset it to history_sync.dispatch_wait (2 min in
// our config), even though no new history arrived. After every bridge restart the stored conversations of logins
// that were still importing sat idle for dispatch_wait. Now a resumed login keeps the short delay while it drains
// the notifications that were already queued when the loop started; only history data that arrives after that
// (the phone is still sending chunks) waits dispatch_wait for more chunks, as before.
type historySyncDispatchDelay struct {
	dispatchWait time.Duration
	// resuming is true from the start of a resumed login's loop until its notification queue was found empty once.
	resuming bool
	wait     time.Duration
}

func newHistorySyncDispatchDelay(dispatchWait time.Duration, resume bool) *historySyncDispatchDelay {
	d := &historySyncDispatchDelay{dispatchWait: dispatchWait, resuming: resume, wait: dispatchWait}
	if resume {
		d.wait = min(historySyncResumeDispatchWait, dispatchWait)
	}
	return d
}

// stored is called after a queued notification was handled. newData is true if it stored conversations that need
// portals (the same condition that re-arms the timer).
func (d *historySyncDispatchDelay) stored(newData bool) {
	if newData && !d.resuming {
		d.wait = d.dispatchWait
	}
}

// drained is called when the notification queue is empty.
func (d *historySyncDispatchDelay) drained() {
	d.resuming = false
}

func (wa *WhatsAppClient) historySyncLoop(ctx context.Context) {
	dispatchTimer := time.NewTimer(wa.Main.Config.HistorySync.DispatchWait)

	var timerPending atomic.Bool
	resume := !wa.isNewLogin && wa.UserLogin.Metadata.(*waid.UserLoginMetadata).HistorySyncPortalsNeedCreating
	delay := newHistorySyncDispatchDelay(wa.Main.Config.HistorySync.DispatchWait, resume)
	if resume {
		dispatchTimer.Reset(delay.wait)
		timerPending.Store(true)
	} else {
		dispatchTimer.Stop()
	}
	if wa.Client.ManualHistorySyncDownload {
		// Wake up the queue once to check if there are pending notifications
		select {
		case wa.historySyncWakeup <- struct{}{}:
		default:
		}
	}
	wa.UserLogin.Log.Debug().Msg("Starting history sync loops")
	// Separate loop for creating portals to ensure it doesn't block processing new history sync payloads.
	go func() {
		for {
			select {
			case <-dispatchTimer.C:
				timerPending.Store(false)
				wa.createPortalsFromHistorySync(ctx)
			case <-ctx.Done():
				wa.UserLogin.Log.Debug().Msg("Stopping portal creation history sync loop")
				return
			}
		}
	}()
	for {
		var resetTimer bool
		select {
		case <-wa.historySyncWakeup:
			dispatchTimer.Stop()
			notif, rowid, err := wa.Main.DB.HSNotif.GetNext(ctx, wa.UserLogin.ID)
			if err != nil {
				wa.UserLogin.Log.Err(err).Msg("Failed to get next history sync notification")
			} else if notif == nil {
				wa.UserLogin.Log.Debug().Msg("No more queued history sync notifications")
				delay.drained()
			} else {
				resetTimer = wa.downloadAndSaveWAHistorySyncData(ctx, notif, rowid)
				delay.stored(resetTimer)
				// Continue waking up the loop until all queued notifications are processed
				select {
				case wa.historySyncWakeup <- struct{}{}:
				default:
				}
			}
		case <-ctx.Done():
			wa.UserLogin.Log.Debug().Msg("Stopping main history sync loop")
			return
		}
		if resetTimer {
			timerPending.Store(true)
		}
		if timerPending.Load() {
			dispatchTimer.Reset(delay.wait)
		}
	}
}

func (wa *WhatsAppClient) saveWAHistorySyncNotification(ctx context.Context, evt *waE2E.HistorySyncNotification) {
	err := wa.Main.DB.HSNotif.Put(ctx, wa.UserLogin.ID, evt)
	if err != nil {
		wa.UserLogin.Log.Err(err).Msg("Failed to store history sync notification in queue")
		return
	}
	wa.UserLogin.Log.Debug().
		Stringer("sync_type", evt.GetSyncType()).
		Uint32("chunk_order", evt.GetChunkOrder()).
		Uint32("progress", evt.GetProgress()).
		Msg("Stored history sync notification in queue")
	select {
	case wa.historySyncWakeup <- struct{}{}:
	default:
	}
}

func (wa *WhatsAppClient) downloadAndSaveWAHistorySyncData(ctx context.Context, evt *waE2E.HistorySyncNotification, rowid int) (resetTimer bool) {
	log := wa.UserLogin.Log.With().
		Str("action", "download history sync").
		Stringer("sync_type", evt.GetSyncType()).
		Uint32("chunk_order", evt.GetChunkOrder()).
		Uint32("progress", evt.GetProgress()).
		Logger()
	log.Debug().
		Int64("oldest_msg_in_chunk_ts", evt.GetOldestMsgInChunkTimestampSec()).
		Any("full_request_meta", evt.GetFullHistorySyncOnDemandRequestMetadata()).
		Any("access_status", evt.GetMessageAccessStatus()).
		Str("peer_data_request_session_id", evt.GetPeerDataRequestSessionID()).
		Msg("Downloading history sync")
	blob, err := wa.Client.DownloadHistorySync(log.WithContext(ctx), evt, true)
	if err != nil {
		log.Err(err).Msg("Failed to download history sync")
		return
	}
	if blob.GetSyncType() == waHistorySync.HistorySync_ON_DEMAND {
		wa.handleOnDemandHistorySync(ctx, blob)
		if err = wa.Main.DB.HSNotif.Delete(ctx, rowid); err != nil {
			log.Err(err).Msg("Failed to delete queued on-demand history sync notification")
		} else if err = wa.Client.DeleteMedia(ctx, whatsmeow.MediaHistory, evt.GetDirectPath(), evt.GetFileEncSHA256(), evt.GetEncHandle()); err != nil {
			log.Err(err).Msg("Failed to delete history sync blob from server")
		} else {
			log.Debug().Msg("Finished handling on-demand history sync and deleted history sync blob from server")
		}
		return
	}
	err = wa.Main.DB.DoTxn(ctx, nil, func(ctx context.Context) (innerErr error) {
		innerErr = wa.handleWAHistorySync(ctx, evt, blob, true)
		if innerErr != nil {
			return
		}
		innerErr = wa.Main.DB.HSNotif.Delete(ctx, rowid)
		if innerErr != nil {
			innerErr = fmt.Errorf("failed to delete queued history sync notification: %w", innerErr)
		}
		return
	})
	if err != nil {
		log.Err(err).Msg("Failed to store history sync notification data")
	} else {
		resetTimer = blob.GetSyncType() == waHistorySync.HistorySync_INITIAL_BOOTSTRAP ||
			blob.GetSyncType() == waHistorySync.HistorySync_RECENT ||
			blob.GetSyncType() == waHistorySync.HistorySync_FULL
		err = wa.Client.DeleteMedia(ctx, whatsmeow.MediaHistory, evt.GetDirectPath(), evt.GetFileEncSHA256(), evt.GetEncHandle())
		if err != nil {
			log.Err(err).Msg("Failed to delete history sync blob from server")
		} else {
			log.Debug().Msg("Deleted history sync blob from server")
		}
	}
	return
}

func (wa *WhatsAppClient) handleWAHistorySync(
	ctx context.Context,
	notif *waE2E.HistorySyncNotification,
	evt *waHistorySync.HistorySync,
	stopOnError bool,
) error {
	if evt == nil || evt.SyncType == nil {
		return nil
	}
	log := wa.UserLogin.Log.With().
		Str("action", "store history sync").
		Stringer("sync_type", evt.GetSyncType()).
		Uint32("chunk_order", evt.GetChunkOrder()).
		Uint32("progress", evt.GetProgress()).
		Logger()
	ctx = log.WithContext(ctx)
	if evt.GetGlobalSettings() != nil {
		log.Debug().Interface("global_settings", evt.GetGlobalSettings()).Msg("Got global settings in history sync")
	}
	if evt.GetSyncType() == waHistorySync.HistorySync_INITIAL_STATUS_V3 ||
		evt.GetSyncType() == waHistorySync.HistorySync_PUSH_NAME ||
		evt.GetSyncType() == waHistorySync.HistorySync_NON_BLOCKING_DATA {
		if evt.GetSyncType() == waHistorySync.HistorySync_PUSH_NAME {
			wa.pushNamesSynced.Set()
		}
		log.Debug().
			Int("conversation_count", len(evt.GetConversations())).
			Int("pushname_count", len(evt.GetPushnames())).
			Int("status_count", len(evt.GetStatusV3Messages())).
			Int("recent_sticker_count", len(evt.GetRecentStickers())).
			Int("past_participant_count", len(evt.GetPastParticipants())).
			Msg("Ignoring history sync")
		return nil
	}
	log.Info().
		Int("conversation_count", len(evt.GetConversations())).
		Int("past_participant_count", len(evt.GetPastParticipants())).
		Dict("notification_metadata", zerolog.Dict().
			Int64("oldest_msg_in_chunk_ts", notif.GetOldestMsgInChunkTimestampSec()).
			Any("full_request_meta", notif.GetFullHistorySyncOnDemandRequestMetadata()).
			Any("access_status", notif.GetMessageAccessStatus()).
			Str("peer_data_request_session_id", notif.GetPeerDataRequestSessionID())).
		Msg("Storing history sync")
	start := time.Now()
	successfullySavedTotal := 0
	failedToSaveTotal := 0
	totalMessageCount := 0
	for _, conv := range evt.GetConversations() {
		log := log.With().
			Int("msg_count", len(conv.GetMessages())).
			Logger()
		jid, err := types.ParseJID(conv.GetID())
		if err != nil {
			totalMessageCount += len(conv.GetMessages())
			log.Warn().Err(err).
				Str("chat_jid", conv.GetID()).
				Msg("Failed to parse chat JID in history sync")
			continue
		} else if jid.Server == types.BroadcastServer {
			log.Debug().Stringer("chat_jid", jid).Msg("Skipping broadcast list in history sync")
			continue
		} else {
			totalMessageCount += len(conv.GetMessages())
		}
		if jid.Server == types.HiddenUserServer {
			pn, err := wa.GetStore().LIDs.GetPNForLID(ctx, jid)
			if err != nil {
				log.Err(err).Stringer("lid", jid).Msg("Failed to get PN for LID in history sync")
			} else if pn.IsEmpty() {
				log.Warn().Stringer("lid", jid).Msg("No PN found for LID in history sync")
			} else {
				log.Debug().
					Stringer("lid", jid).
					Stringer("pn", pn).
					Msg("Rerouting LID DM to phone number in history sync")
				jid = pn
			}
		}
		log.UpdateContext(func(c zerolog.Context) zerolog.Context {
			return c.Stringer("chat_jid", jid)
		})

		var minTime, maxTime, firstItemTime, lastItemTime time.Time
		var minTimeIndex, maxTimeIndex int

		ignoredTypes := 0
		messages := make([]*wadb.HistorySyncMessageTuple, 0, len(conv.GetMessages()))
		for i, rawMsg := range conv.GetMessages() {
			// Don't store messages that will just be skipped.
			msgEvt, err := wa.Client.ParseWebMessage(jid, rawMsg.GetMessage())
			if err != nil {
				log.Warn().Err(err).
					Int("msg_index", i).
					Str("msg_id", rawMsg.GetMessage().GetKey().GetID()).
					Uint64("msg_time_seconds", rawMsg.GetMessage().GetMessageTimestamp()).
					Msg("Dropping historical message due to parse error")
				log.Trace().
					Any("web_message_info", rawMsg.GetMessage()).
					Msg("Content of event that failed to parse")
				continue
			}
			if firstItemTime.IsZero() {
				firstItemTime = msgEvt.Info.Timestamp
			}
			lastItemTime = msgEvt.Info.Timestamp
			if minTime.IsZero() || msgEvt.Info.Timestamp.Before(minTime) {
				minTime = msgEvt.Info.Timestamp
				minTimeIndex = i
			}
			if maxTime.IsZero() || msgEvt.Info.Timestamp.After(maxTime) {
				maxTime = msgEvt.Info.Timestamp
				maxTimeIndex = i
			}

			msgType := getMessageType(msgEvt.Message)
			if msgType == "ignore" || strings.HasPrefix(msgType, "unknown_protocol_") {
				ignoredTypes++
				continue
			}
			marshaled, err := proto.Marshal(rawMsg)
			if err != nil {
				log.Warn().Err(err).
					Int("msg_index", i).
					Str("msg_id", msgEvt.Info.ID).
					Msg("Failed to marshal message")
				continue
			}

			messages = append(messages, &wadb.HistorySyncMessageTuple{Info: &msgEvt.Info, Message: marshaled})
		}
		log.Debug().
			Int("wrapped_count", len(messages)).
			Int("ignored_msg_type_count", ignoredTypes).
			Time("lowest_time", minTime).
			Int("lowest_time_index", minTimeIndex).
			Time("highest_time", maxTime).
			Int("highest_time_index", maxTimeIndex).
			Time("first_item_time", firstItemTime).
			Time("last_item_time", lastItemTime).
			Bool("highest_time_mismatch", firstItemTime != maxTime).
			Dict("metadata", zerolog.Dict().
				Uint32("ephemeral_expiration", conv.GetEphemeralExpiration()).
				Int64("ephemeral_setting_timestamp", conv.GetEphemeralSettingTimestamp()).
				Uint64("last_message_ts", conv.GetLastMsgTimestamp()).
				Bool("marked_unread", conv.GetMarkedAsUnread()).
				Bool("archived", conv.GetArchived()).
				Uint32("pinned", conv.GetPinned()).
				Uint64("mute_end", conv.GetMuteEndTime()).
				Uint32("unread_count", conv.GetUnreadCount()).
				Bool("end_of_history", conv.GetEndOfHistoryTransfer()).
				Stringer("end_of_history_type", conv.GetEndOfHistoryTransferType()),
			).
			Msg("Collected messages to save from history sync conversation")

		if len(messages) > 0 {
			err = wa.Main.DB.Conversation.Put(ctx, wadb.NewConversation(wa.UserLogin.ID, jid, conv, maxTime))
			if err != nil {
				if stopOnError {
					return fmt.Errorf("failed to save conversation metadata for %s: %w", jid, err)
				}
				log.Err(err).Msg("Failed to save conversation metadata")
				continue
			}
			err = wa.Main.DB.Message.Put(ctx, wa.UserLogin.ID, jid, messages)
			if err != nil {
				if stopOnError {
					return fmt.Errorf("failed to save messages in %s: %w", jid, err)
				}
				log.Err(err).Msg("Failed to save messages")
				failedToSaveTotal += len(messages)
			} else {
				successfullySavedTotal += len(messages)
			}
			err = wa.Main.Bridge.DB.BackfillTask.MarkNotDone(ctx, wa.makeWAPortalKey(jid), wa.UserLogin.ID)
			if err != nil {
				if stopOnError {
					return fmt.Errorf("failed to mark backfill task as not done for %s: %w", jid, err)
				}
				log.Err(err).Msg("Failed to mark backfill task as not done")
			}
		}
	}
	log.Info().
		Int("total_saved_count", successfullySavedTotal).
		Int("total_failed_count", failedToSaveTotal).
		Int("total_message_count", totalMessageCount).
		Dur("duration", time.Since(start)).
		Msg("Finished storing history sync")
	return nil
}

func (wa *WhatsAppClient) createPortalsFromHistorySync(ctx context.Context) {
	log := wa.UserLogin.Log.With().
		Str("action", "create portals from history sync").
		Logger()
	ctx = log.WithContext(ctx)
	if !wa.isCurrent() {
		log.Debug().Msg("Not creating portals from history sync on a replaced or logged out client instance")
		return
	}
	meta := wa.UserLogin.Metadata.(*waid.UserLoginMetadata)
	// loginTS is the current pairing: conversations handled in this pairing are marked as synced with it.
	loginTS := meta.LoggedInAt
	// intbox: importEpoch is the start of the account's history import. It's the same as loginTS, unless this
	// pairing is a re-pair within intbox.reimport_after. Then conversations imported earlier in the epoch only
	// get a catch-up (forward backfill of missing messages, no group info lookup, no member sync), and the
	// per-login room cap counts every room created in the epoch.
	importEpoch := meta.GetImportEpoch()
	epochCarried := meta.ImportEpochCarried()
	limit := wa.Main.Config.HistorySync.MaxInitialConversations
	maxRoomsPerLogin := wa.Main.Config.HistorySync.MaxRoomsPerLogin
	fetchLimit := limit
	remainingRooms := -1
	var inFlight map[types.JID]struct{}
	if maxRoomsPerLogin > 0 {
		syncedCount, err := wa.Main.DB.Conversation.CountSynced(ctx, wa.UserLogin.ID, importEpoch)
		if err != nil {
			log.Err(err).Msg("Failed to count conversations already bridged for this login")
			return
		}
		inFlight = wa.historySyncPortals.getInFlight(importEpoch)
		remainingRooms = max(maxRoomsPerLogin-syncedCount-len(inFlight), 0)
		if remainingRooms <= 0 && !epochCarried {
			log.Info().
				Int("max_rooms_per_login", maxRoomsPerLogin).
				Int("synced_count", syncedCount).
				Int("in_flight_count", len(inFlight)).
				Msg("Per-login room limit reached, not creating more portals from history sync")
			return
		}
		if !epochCarried && (limit < 0 || limit > remainingRooms) {
			// Every candidate is a new room, so don't fetch more than may be created.
			// After a re-pair, candidates that already have a room don't count against the limit.
			limit = remainingRooms
		}
		if limit >= 0 {
			// Conversations whose portals are still being created haven't been marked as synced yet,
			// so they're returned again. Fetch extra rows so they can be skipped below.
			fetchLimit = limit + len(inFlight)
		}
	}
	candidates, err := wa.Main.DB.Conversation.GetImportCandidates(ctx, wa.UserLogin.ID, fetchLimit, loginTS)
	if err != nil {
		log.Err(err).Msg("Failed to get recent conversations from database")
		return
	}
	maxNewRooms := limit
	if maxRoomsPerLogin > 0 && (maxNewRooms < 0 || maxNewRooms > remainingRooms) {
		maxNewRooms = remainingRooms
	}
	conversations, catchUps, alreadyImported := partitionImportCandidates(
		candidates, inFlight, epochCarried, importEpoch, maxNewRooms,
		func(chatJID types.JID) bool { return wa.hasPortalRoomForLogin(ctx, wa.makeWAPortalKey(chatJID)) },
	)
	for _, conv := range alreadyImported {
		// Imported earlier in this epoch, but the room is gone (e.g. the chat was deleted): don't recreate it.
		if err = wa.Main.DB.Conversation.MarkSynced(ctx, wa.UserLogin.ID, conv.ChatJID, loginTS); err != nil {
			log.Err(err).Stringer("chat_jid", conv.ChatJID).Msg("Failed to mark conversation as handled")
		}
	}
	log.Info().
		Int("limit", limit).
		Int("max_rooms_per_login", maxRoomsPerLogin).
		Int("conversation_count", len(conversations)).
		Int("catch_up_count", len(catchUps)).
		Int("already_imported_count", len(alreadyImported)).
		Bool("import_epoch_carried", epochCarried).
		Int64("login_timestamp", loginTS.Unix()).
		Int64("import_epoch", importEpoch.Unix()).
		Msg("Creating portals from history sync")
	// intbox: the catch-ups aren't rooms of the import round below, but HistorySyncPortalsNeedCreating has to stay
	// set until they were handled too. They only exist in portal queues, so after a restart only this function
	// can queue the ones that weren't handled yet (handled ones are marked as synced with loginTS).
	catchUpsHandled, catchUpsQueued := closedChan, true
	if len(catchUps) > 0 {
		catchUpsHandled, catchUpsQueued = wa.queueHistorySyncCatchUps(ctx, catchUps, loginTS)
	}
	paceGroupLookups := wa.Main.Config.HistorySync.GroupInfoLookupsPerSecond > 0
	rateLimitErrors := 0
	// intbox W4: wait for the admission queue; wg counts done rooms like a sync.WaitGroup (see intbox_admission.go)
	// L7-1: the admission lanes (small/heavy imports) are sized by the login's buffered history messages
	bufferedMessages := -1
	if countLimit := intboxImportMessageCountLimit(&wa.Main.Config.Intbox); countLimit > 0 && len(conversations) > 0 {
		bufferedMessages, err = wa.Main.DB.Message.CountHistorySyncMessages(ctx, wa.UserLogin.ID, countLimit)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to count buffered history sync messages, admitting the import in the main lane")
			bufferedMessages = -1
		}
	}
	wg, admitted := wa.intboxAdmitImport(ctx, len(conversations), bufferedMessages)
	if !admitted {
		log.Warn().Err(ctx.Err()).Msg("Context cancelled while waiting for history sync import admission")
		return
	}
	for i := 0; i < len(conversations); i++ {
		if ctx.Err() != nil {
			log.Warn().Err(ctx.Err()).Msg("Context cancelled, stopping history sync portal creation")
			return
		} else if wa.Client == nil {
			log.Warn().Msg("Client is nil, stopping history sync portal creation")
			return
		} else if !wa.isCurrent() {
			log.Warn().Msg("Client instance was replaced or logged out, stopping history sync portal creation")
			return
		}
		conv := conversations[i]
		if conv.ChatJID == types.StatusBroadcastJID && !wa.Main.Config.EnableStatusBroadcast {
			wg.Done()
			continue
		} else if conv.ChatJID == types.PSAJID || conv.ChatJID == types.LegacyPSAJID {
			// We don't currently support new PSAs, so don't bother backfilling them either
			wg.Done()
			continue
		}
		// TODO can the chat info fetch be avoided entirely?
		wait := time.Duration(rateLimitErrors) * time.Second
		isGroupLookup := conv.ChatJID.Server == types.GroupServer
		if paceGroupLookups {
			// Only group info lookups are IQs that can hit the rate limit, DMs don't need to wait at all.
			if isGroupLookup {
				wait = max(wait, wa.historySyncPortals.groupLookupDelay(importEpoch, wa.Main.Config.HistorySync.GroupInfoLookupsPerSecond))
			} else {
				wait = 0
			}
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			log.Warn().Err(ctx.Err()).Msg("Context cancelled, stopping history sync portal creation")
			return
		}
		if paceGroupLookups && isGroupLookup {
			wa.historySyncPortals.markGroupLookup(importEpoch)
		}
		wrappedInfo, err := wa.getChatInfo(ctx, conv.ChatJID, conv, true)
		if errors.Is(err, whatsmeow.ErrNotInGroup) {
			log.Debug().Stringer("chat_jid", conv.ChatJID).
				Msg("Skipping creating room because the user is not a participant")
			//err = wa.Main.DB.Message.DeleteAllInChat(ctx, wa.UserLogin.ID, conv.ChatJID)
			//if err != nil {
			//	log.Err(err).Msg("Failed to delete historical messages for portal")
			//}
			err = wa.Main.DB.Conversation.Delete(ctx, wa.UserLogin.ID, conv.ChatJID)
			if err != nil {
				log.Err(err).Msg("Failed to delete conversation user is not in")
			}
			wg.Done()
			continue
		} else if errors.Is(err, whatsmeow.ErrIQRateOverLimit) {
			rateLimitErrors++
			if paceGroupLookups {
				// Pace all following lookups even if the rate limit was hit before the unpaced burst was used up
				wa.historySyncPortals.startPacing(importEpoch)
			}
			i--
			log.Err(err).Stringer("chat_jid", conv.ChatJID).
				Int("error_count", rateLimitErrors).
				Msg("Ratelimit error getting chat info, retrying after sleep")
			select {
			case <-time.After(time.Duration(rateLimitErrors) * time.Second):
			case <-ctx.Done():
				log.Warn().Err(ctx.Err()).Msg("Context cancelled, stopping history sync portal creation")
				return
			}
			continue
		} else if err != nil {
			log.Err(err).Stringer("chat_jid", conv.ChatJID).Msg("Failed to get chat info")
			wg.Done()
			continue
		}
		if paceGroupLookups {
			// Lookups are paced, so there's no need to keep sleeping before every conversation after one rate limit error.
			rateLimitErrors = 0
		}
		if maxRoomsPerLogin > 0 {
			wa.historySyncPortals.addInFlight(importEpoch, conv.ChatJID)
		}
		// intbox: wrapped so that the event does nothing if this instance is replaced before the portal loop handles it
		res := wa.UserLogin.QueueRemoteEvent(&historySyncChatResync{wa: wa, ChatResync: &simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type: bridgev2.RemoteEventChatResync,
				LogContext: func(c zerolog.Context) zerolog.Context {
					return c.
						Stringer("chat_jid", conv.ChatJID).
						Time("latest_message_ts", conv.LastMessageTimestamp)
				},
				PortalKey:    wa.makeWAPortalKey(conv.ChatJID),
				CreatePortal: true,
				PostHandleFunc: func(ctx context.Context, portal *bridgev2.Portal) {
					// intbox: only the current instance marks conversations as synced
					if wa.isCurrent() {
						err := wa.Main.DB.Conversation.MarkSynced(ctx, wa.UserLogin.ID, conv.ChatJID, loginTS)
						if err != nil {
							zerolog.Ctx(ctx).Err(err).Msg("Failed to mark conversation as bridged")
						}
					}
					if maxRoomsPerLogin > 0 {
						wa.historySyncPortals.removeInFlight(importEpoch, conv.ChatJID)
					}
					wg.Done()
				},
			},
			ChatInfo:        wrappedInfo,
			LatestMessageTS: conv.LastMessageTimestamp,
		}})
		if !res.Success {
			if maxRoomsPerLogin > 0 {
				wa.historySyncPortals.removeInFlight(importEpoch, conv.ChatJID)
			}
			log.Debug().Msg("Cancelling history sync portal creation loop")
			return
		}
	}
	log.Info().Int("conversation_count", len(conversations)).Msg("Finished creating portals from history sync")
	if !catchUpsQueued {
		log.Warn().Msg("Not all history sync catch-ups could be queued, keeping the history sync portals flag set")
		return
	}
	lifeCtx := wa.lifeContext()
	go func() {
		// intbox: this used to block forever on the WaitGroup and then save the flag of whichever login object
		// it had, even after the instance had been replaced. It now stops when the instance is retired, and only
		// the current instance clears the flag. It also waits for the catch-ups, which wg doesn't count.
		for _, done := range []<-chan struct{}{wg.DoneChan(), catchUpsHandled} {
			select {
			case <-done:
			case <-lifeCtx.Done():
				log.Debug().Msg("Client instance was retired before all history sync chat resync events were handled")
				return
			}
		}
		if !wa.isCurrent() {
			log.Debug().Msg("Not saving history sync portals created flag, the client instance was replaced")
			return
		}
		wa.UserLogin.Metadata.(*waid.UserLoginMetadata).HistorySyncPortalsNeedCreating = false
		err := wa.UserLogin.Save(context.WithoutCancel(ctx))
		if err != nil {
			log.Err(err).Msg("Failed to save user login history sync portals created flag")
		}
		log.Info().Msg("Finished processing all history sync chat resync events")
	}()
}

// partitionImportCandidates splits the conversations that haven't been handled in the current pairing:
//   - conversations: get a room created from history sync (chat info, group info lookup, member sync), at most
//     maxNewRooms of them (-1 = no limit), most recent first;
//   - catchUps: re-pair within intbox.reimport_after (epochCarried) and the room exists: forward backfill only;
//   - alreadyImported: re-pair, imported earlier in the epoch but the room is gone: nothing to do.
//
// Without a carried epoch every candidate is a new room, exactly like before import epochs existed.
// Conversations whose portal is still being created (inFlight) are skipped.
func partitionImportCandidates(
	candidates []*wadb.ImportCandidate, inFlight map[types.JID]struct{}, epochCarried bool, importEpoch jsontime.Unix,
	maxNewRooms int, hasRoom func(types.JID) bool,
) (conversations, catchUps, alreadyImported []*wadb.Conversation) {
	conversations = make([]*wadb.Conversation, 0, len(candidates))
	for _, cand := range candidates {
		if _, isInFlight := inFlight[cand.ChatJID]; isInFlight {
			continue
		}
		if !epochCarried {
			conversations = append(conversations, cand.Conversation)
		} else if hasRoom(cand.ChatJID) {
			catchUps = append(catchUps, cand.Conversation)
		} else if cand.SyncedSince(importEpoch) {
			alreadyImported = append(alreadyImported, cand.Conversation)
		} else {
			conversations = append(conversations, cand.Conversation)
		}
	}
	if maxNewRooms >= 0 && len(conversations) > maxNewRooms {
		conversations = conversations[:maxNewRooms]
	}
	return
}

// hasPortalRoomForLogin returns true if the portal has a Matrix room and this login is in it (has a user portal).
func (wa *WhatsAppClient) hasPortalRoomForLogin(ctx context.Context, portalKey networkid.PortalKey) bool {
	portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Object("portal_key", portalKey).Msg("Failed to get portal to check if it exists")
		return false
	} else if portal == nil || portal.MXID == "" {
		return false
	}
	userPortal, err := wa.Main.Bridge.DB.UserPortal.Get(ctx, wa.UserLogin.UserLogin, portalKey)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Object("portal_key", portalKey).Msg("Failed to get user portal to check if login is in portal")
		return false
	}
	return userPortal != nil
}

// queueHistorySyncCatchUps handles the conversations of a re-pair (within intbox.reimport_after) that already
// have a room. Messages that arrived while no device was linked only exist in the new device's history sync,
// so they're backfilled forward from the newest bridged message. The resync carries no chat info, so there's
// no group info lookup and no member sync. Conversations whose room already has the newest message are only
// marked as handled.
//
// handled is closed once every queued catch-up was handled by its portal (its PostHandleFunc ran). complete is
// false if the loop stopped before every conversation was queued; handled is then nil and is never closed.
// A queued catch-up that its portal drops without handling (e.g. the room was deleted meanwhile) keeps handled
// open, which only means that HistorySyncPortalsNeedCreating stays set until the next start re-checks the chats.
func (wa *WhatsAppClient) queueHistorySyncCatchUps(
	ctx context.Context, conversations []*wadb.Conversation, loginTS jsontime.Unix,
) (handled <-chan struct{}, complete bool) {
	log := zerolog.Ctx(ctx)
	pending := newPendingResyncs()
	queued, upToDate := 0, 0
	for _, conv := range conversations {
		if ctx.Err() != nil || !wa.isCurrent() {
			log.Warn().Msg("Stopping history sync catch-up, the client instance was replaced or the loop was stopped")
			return nil, false
		}
		portalKey := wa.makeWAPortalKey(conv.ChatJID)
		if !wa.historySyncCatchUpNeeded(ctx, portalKey, conv) {
			upToDate++
			if err := wa.Main.DB.Conversation.MarkSynced(ctx, wa.UserLogin.ID, conv.ChatJID, loginTS); err != nil {
				log.Err(err).Stringer("chat_jid", conv.ChatJID).Msg("Failed to mark conversation as handled")
			}
			continue
		}
		markHandled := pending.add()
		res := wa.UserLogin.QueueRemoteEvent(&historySyncChatResync{wa: wa, ChatResync: &simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type: bridgev2.RemoteEventChatResync,
				LogContext: func(c zerolog.Context) zerolog.Context {
					return c.
						Str("sync_reason", "re-pair catch-up").
						Stringer("chat_jid", conv.ChatJID).
						Time("latest_message_ts", conv.LastMessageTimestamp)
				},
				PortalKey: portalKey,
				PostHandleFunc: func(ctx context.Context, portal *bridgev2.Portal) {
					defer markHandled()
					if !wa.isCurrent() {
						return
					}
					if err := wa.Main.DB.Conversation.MarkSynced(ctx, wa.UserLogin.ID, conv.ChatJID, loginTS); err != nil {
						zerolog.Ctx(ctx).Err(err).Msg("Failed to mark conversation as handled")
					}
				},
			},
			LatestMessageTS: conv.LastMessageTimestamp,
		}})
		if !res.Queued {
			// Handled synchronously (PostHandleFunc already ran), ignored or failed: nothing is left in a portal
			// queue. markHandled does nothing if PostHandleFunc already called it.
			markHandled()
		}
		if !res.Success {
			log.Debug().Msg("Cancelling history sync catch-up loop")
			return nil, false
		}
		queued++
	}
	log.Info().
		Int("queued_count", queued).
		Int("up_to_date_count", upToDate).
		Msg("Queued catch-up backfills for conversations that already have a room")
	return pending.seal(), true
}

// closedChan is an already closed channel, for "nothing to wait for".
var closedChan = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// pendingResyncs counts queued resync events, like a sync.WaitGroup whose Wait is a channel, without a goroutine
// that would block forever if an event is dropped without being handled.
type pendingResyncs struct {
	lock    sync.Mutex
	pending int
	sealed  bool
	done    chan struct{}
}

func newPendingResyncs() *pendingResyncs {
	return &pendingResyncs{done: make(chan struct{})}
}

// add counts one more event. The returned function marks it as handled; calls after the first do nothing.
func (pr *pendingResyncs) add() func() {
	pr.lock.Lock()
	pr.pending++
	pr.lock.Unlock()
	return sync.OnceFunc(func() {
		pr.lock.Lock()
		defer pr.lock.Unlock()
		pr.pending--
		pr.closeIfDoneLocked()
	})
}

// seal is called after the last add. It returns a channel that is closed once every added event was handled.
func (pr *pendingResyncs) seal() <-chan struct{} {
	pr.lock.Lock()
	defer pr.lock.Unlock()
	pr.sealed = true
	pr.closeIfDoneLocked()
	return pr.done
}

func (pr *pendingResyncs) closeIfDoneLocked() {
	if !pr.sealed || pr.pending > 0 {
		return
	}
	select {
	case <-pr.done:
	default:
		close(pr.done)
	}
}

// historySyncCatchUpNeeded returns true if the history sync has messages newer than the newest bridged message.
func (wa *WhatsAppClient) historySyncCatchUpNeeded(ctx context.Context, portalKey networkid.PortalKey, conv *wadb.Conversation) bool {
	if conv.LastMessageTimestamp.IsZero() {
		return false
	}
	latest, err := wa.Main.Bridge.DB.Message.GetLastPartAtOrBeforeTime(ctx, portalKey, time.Now().Add(10*time.Second))
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Stringer("chat_jid", conv.ChatJID).Msg("Failed to get last message to check if catch-up is needed")
		return true
	}
	return latest == nil || conv.LastMessageTimestamp.After(latest.Timestamp)
}

// unpacedGroupInfoLookups is the number of group info lookups per login that are never paced by
// history_sync.group_info_lookups_per_second. WhatsApp's per-account burst limit has only been seen
// after the 26th lookup.
const unpacedGroupInfoLookups = 20

// historySyncPortalState is the in-memory state of portal creation from history sync for one login.
// It's reset whenever the given timestamp changes. Callers pass the import epoch (intbox), which only
// changes on a pairing that starts a new import. The state lives on the client instance, so it's also
// fresh after every pairing and restart.
type historySyncPortalState struct {
	lock            sync.Mutex
	loginTS         jsontime.Unix
	inFlight        map[types.JID]struct{}
	groupLookups    int
	lastGroupLookup time.Time
}

func (hs *historySyncPortalState) resetIfNeeded(loginTS jsontime.Unix) {
	if hs.inFlight == nil || !hs.loginTS.Equal(loginTS.Time) {
		hs.loginTS = loginTS
		hs.inFlight = make(map[types.JID]struct{})
		hs.groupLookups = 0
		hs.lastGroupLookup = time.Time{}
	}
}

func (hs *historySyncPortalState) getInFlight(loginTS jsontime.Unix) map[types.JID]struct{} {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	hs.resetIfNeeded(loginTS)
	return maps.Clone(hs.inFlight)
}

func (hs *historySyncPortalState) addInFlight(loginTS jsontime.Unix, chatJID types.JID) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	hs.resetIfNeeded(loginTS)
	hs.inFlight[chatJID] = struct{}{}
}

func (hs *historySyncPortalState) removeInFlight(loginTS jsontime.Unix, chatJID types.JID) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	if hs.inFlight != nil && hs.loginTS.Equal(loginTS.Time) {
		delete(hs.inFlight, chatJID)
	}
}

// groupLookupDelay returns how long to wait before the next group info lookup to stay under perSecond lookups per second.
func (hs *historySyncPortalState) groupLookupDelay(loginTS jsontime.Unix, perSecond float64) time.Duration {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	hs.resetIfNeeded(loginTS)
	if hs.groupLookups < unpacedGroupInfoLookups || hs.lastGroupLookup.IsZero() {
		return 0
	}
	interval := time.Duration(float64(time.Second) / perSecond)
	return max(time.Until(hs.lastGroupLookup.Add(interval)), 0)
}

func (hs *historySyncPortalState) startPacing(loginTS jsontime.Unix) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	hs.resetIfNeeded(loginTS)
	hs.groupLookups = max(hs.groupLookups, unpacedGroupInfoLookups)
}

func (hs *historySyncPortalState) markGroupLookup(loginTS jsontime.Unix) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	hs.resetIfNeeded(loginTS)
	hs.groupLookups++
	hs.lastGroupLookup = time.Now()
}

func (wa *WhatsAppClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	portalJID, err := waid.ParsePortalID(params.Portal.ID)
	if err != nil {
		return nil, err
	}
	params.Count = wa.intboxMatrixBackfillCount(params) // intbox L1: history bypass keeps only the newest messages for Matrix (intbox_history.go)
	var markRead bool
	var startTime, endTime *time.Time
	var conv *wadb.Conversation
	if params.Forward || wa.Main.Config.HistorySync.BackwardsOnDemand {
		conv, err = wa.Main.DB.Conversation.Get(ctx, wa.UserLogin.ID, portalJID)
		if err != nil {
			return nil, fmt.Errorf("failed to get conversation from database: %w", err)
		}
	}
	if params.Forward {
		if params.AnchorMessage != nil {
			startTime = ptr.Ptr(params.AnchorMessage.Timestamp)
		} else if maxAge := wa.Main.Config.HistorySync.MaxInitialAge; maxAge > 0 {
			// There's no anchor only in the initial forward backfill of an empty portal (room creation or
			// resync of an empty portal). Catch-up backfills always have an anchor and aren't limited.
			startTime = ptr.Ptr(time.Now().Add(-maxAge))
		}
		if conv != nil {
			markRead = !ptr.Val(conv.MarkedAsUnread) && ptr.Val(conv.UnreadCount) == 0
		}
	} else {
		if params.AnchorMessage != nil {
			endTime = ptr.Ptr(params.AnchorMessage.Timestamp)
		}
		if params.Cursor != "" {
			endTimeUnix, err := strconv.ParseInt(string(params.Cursor), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse cursor: %w", err)
			}
			cursorTime := time.Unix(endTimeUnix, 0)
			if endTime == nil || cursorTime.Before(*endTime) {
				endTime = &cursorTime
			}
		}
	}
	var anchorID types.MessageID
	if params.AnchorMessage != nil {
		parsedID, _ := waid.ParseMessageID(params.AnchorMessage.ID)
		if parsedID != nil {
			anchorID = parsedID.ID
		}
	}
	var hasMore bool
	if !params.Forward && wa.Main.Config.HistorySync.BackwardsOnDemand {
		hasMore = conv != nil && ptr.Val(conv.EndOfHistoryTransferType) == waHistorySync.Conversation_COMPLETE_BUT_MORE_MESSAGES_REMAIN_ON_PRIMARY
	}
	messages, err := wa.Main.DB.Message.GetBetween(ctx, wa.UserLogin.ID, portalJID, startTime, endTime, params.Count+1)
	if err != nil {
		return nil, fmt.Errorf("failed to load messages from database: %w", err)
	} else if len(messages) == 0 || (len(messages) == 1 && anchorID != "" && messages[0].GetKey().GetID() == anchorID) {
		wa.deleteHistorySyncMessages(ctx, portalJID, 0, 0)
		if hasMore && !params.AllowSlowFetch {
			return &bridgev2.FetchMessagesResponse{
				MoreRequiresSlowFetch: true,
				HasMore:               true,
				Forward:               params.Forward,
			}, nil
		} else if hasMore {
			return wa.fetchMessagesFromPhone(ctx, params)
		}
		return &bridgev2.FetchMessagesResponse{
			HasMore: false,
			Forward: params.Forward,
		}, nil
	}
	if len(messages) > params.Count {
		oldestTS := messages[len(messages)-1].GetMessageTimestamp()
		hasMore = true
		// For safety, cut off messages with the oldest timestamp in the response.
		// Otherwise, if there are multiple messages with the same timestamp, the next fetch may miss some.
		for i := len(messages) - 2; i >= 0; i-- {
			if messages[i].GetMessageTimestamp() > oldestTS {
				messages = messages[:i+1]
				break
			}
		}
	}
	resp, err := wa.convertHistorySyncMessages(ctx, params.Portal, portalJID, messages, true)
	if err != nil {
		return nil, fmt.Errorf("failed to convert messages: %w", err)
	}
	wa.intboxWrapHistoryBypass(ctx, params, portalJID, messages[len(messages)-1].GetMessageTimestamp(), resp) // intbox L1: move older history to the bypass table, capped (intbox_history.go)
	resp.HasMore = hasMore
	resp.Forward = params.Forward
	resp.MarkRead = markRead
	return resp, nil
}

func (wa *WhatsAppClient) deleteHistorySyncMessages(ctx context.Context, portalJID types.JID, newestTS, oldestTS uint64) {
	var err error
	var rows int64
	if (newestTS == 0 && oldestTS == 0) || !wa.Main.Bridge.Config.Backfill.Queue.AnyEnabled() {
		// If the backfill queue isn't enabled, delete all messages after backfilling a batch.
		rows, err = wa.Main.DB.Message.DeleteAllInChat(ctx, wa.UserLogin.ID, portalJID)
	} else {
		// Otherwise just delete the messages that got backfilled
		rows, err = wa.Main.DB.Message.DeleteBetween(ctx, wa.UserLogin.ID, portalJID, newestTS, oldestTS)
	}
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Stringer("portal_jid", portalJID).
			Uint64("newest_ts", newestTS).
			Uint64("oldest_ts", oldestTS).
			Msg("Failed to delete messages from database after backfill")
	} else {
		zerolog.Ctx(ctx).Debug().
			Stringer("portal_jid", portalJID).
			Uint64("newest_ts", newestTS).
			Uint64("oldest_ts", oldestTS).
			Int64("rows_affected", rows).
			Msg("Deleted history sync messages from database")
	}
}

func (wa *WhatsAppClient) convertHistorySyncMessages(
	ctx context.Context,
	portal *bridgev2.Portal,
	portalJID types.JID,
	messages []*waWeb.WebMessageInfo,
	explodeOnError bool,
) (*bridgev2.FetchMessagesResponse, error) {
	// Upstream 9361103 (#954): don't panic when the whatsmeow client is nil (logged out)
	if wa.Client == nil {
		return nil, bridgev2.ErrNotLoggedIn
	}
	oldestTS := messages[len(messages)-1].GetMessageTimestamp()
	newestTS := messages[0].GetMessageTimestamp()
	convertedMessages := make([]*bridgev2.BackfillMessage, 0, len(messages))
	var mediaRequests []*wadb.MediaRequest
	for i, msg := range messages {
		evt, err := wa.Client.ParseWebMessage(portalJID, msg)
		if err != nil {
			if explodeOnError {
				// This should never happen because the info is already parsed once before being stored in the database
				return nil, fmt.Errorf("failed to parse info of message %s: %w", msg.GetKey().GetID(), err)
			}
			zerolog.Ctx(ctx).Warn().Err(err).
				Int("msg_index", i).
				Str("msg_id", msg.GetKey().GetID()).
				Uint64("msg_time_seconds", msg.GetMessageTimestamp()).
				Msg("Dropping historical message due to parse error")
			zerolog.Ctx(ctx).Trace().
				Any("web_message_info", msg.GetMessage()).
				Msg("Content of event that failed to parse")
			continue
		}
		if !explodeOnError {
			msgType := getMessageType(evt.Message)
			if msgType == "ignore" || strings.HasPrefix(msgType, "unknown_protocol_") {
				continue
			}
		}
		isViewOnce := evt.IsViewOnce || evt.IsViewOnceV2 || evt.IsViewOnceV2Extension
		converted, mediaReq := wa.convertHistorySyncMessage(
			ctx, portal, &evt.Info, evt.Message, evt.RawMessage, isViewOnce, msg.Reactions,
		)
		convertedMessages = append(convertedMessages, converted)
		if mediaReq != nil {
			mediaRequests = append(mediaRequests, mediaReq)
		}
	}
	slices.Reverse(convertedMessages)
	return &bridgev2.FetchMessagesResponse{
		Messages: convertedMessages,
		Cursor:   networkid.PaginationCursor(strconv.FormatUint(oldestTS, 10)),
		CompleteCallback: func() {
			// TODO this only deletes after backfilling. If there's no need for backfill after a relogin,
			//      the messages will be stuck in the database
			wa.intboxAfterBackfill(len(convertedMessages)) // intbox W4 progress (intbox_history.go)
			wa.deleteHistorySyncMessages(ctx, portalJID, newestTS, oldestTS)
			if len(mediaRequests) > 0 {
				go func(ctx context.Context) {
					for _, req := range mediaRequests {
						err := wa.Main.DB.MediaRequest.Put(ctx, req)
						if err != nil {
							zerolog.Ctx(ctx).Err(err).Msg("Failed to save media request to database")
						}
						// intbox: the requests are stored for the next instance, but only the current one sends them
						if wa.Main.Config.HistorySync.MediaRequests.AutoRequestMedia && wa.Main.Config.HistorySync.MediaRequests.RequestMethod == MediaRequestMethodImmediate &&
							wa.lifeContext().Err() == nil && wa.isCurrent() {
							wa.sendMediaRequest(ctx, req)
						}
					}
				}(context.WithoutCancel(ctx))
			}
		},
	}, nil
}

func (wa *WhatsAppClient) fetchMessagesFromPhone(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.AnchorMessage == nil {
		return nil, fmt.Errorf("anchor message is required to fetch messages from phone")
	}
	parsed, err := waid.ParseMessageID(params.AnchorMessage.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse anchor message ID: %w", err)
	}

	msgID := wa.Client.GenerateMessageID()
	reqData := wa.Client.BuildHistorySyncRequest(&types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     parsed.Chat,
			Sender:   parsed.Sender,
			IsFromMe: parsed.Sender.ToNonAD() == wa.JID.ToNonAD() || parsed.Sender.ToNonAD() == wa.Device.GetLID().ToNonAD(),
			IsGroup:  parsed.Chat.Server == types.GroupServer,
		},
		ID:        parsed.ID,
		Timestamp: params.AnchorMessage.Timestamp,
	}, 50)
	zerolog.Ctx(ctx).Debug().
		Str("request_msg_id", msgID).
		Any("anchor_msg_parsed", parsed).
		Any("request_data", reqData).
		Msg("Sending history sync request")
	_, err = wa.Client.SendMessage(ctx, wa.JID.ToNonAD(), reqData, whatsmeow.SendRequestExtra{
		ID:   msgID,
		Peer: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send history sync request: %w", err)
	}
	return &bridgev2.FetchMessagesResponse{
		HasMore: true,
		Pending: true,
	}, nil
}

func (wa *WhatsAppClient) handleOnDemandHistorySync(ctx context.Context, blob *waHistorySync.HistorySync) {
	if len(blob.GetConversations()) > 1 {
		zerolog.Ctx(ctx).Warn().
			Int("conversation_count", len(blob.GetConversations())).
			Msg("Received on-demand history sync with multiple conversations")
	}
	for _, conv := range blob.GetConversations() {
		portalJID, err := types.ParseJID(conv.GetID())
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Str("jid", conv.GetID()).Msg("Failed to parse portal JID")
			continue
		}
		portal, err := wa.Main.Bridge.GetPortalByKey(ctx, wa.makeWAPortalKey(portalJID))
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Stringer("portal_jid", portalJID).Msg("Failed to get portal for on-demand history sync")
			continue
		}
		ctx := zerolog.Ctx(ctx).With().
			Str("portal_id", string(portal.ID)).
			Str("portal_receiver", string(portal.Receiver)).
			Stringer("portal_mxid", portal.MXID).
			Logger().WithContext(ctx)
		portal.HandleRemoteBackfill(ctx, wa.UserLogin, &simplevent.Backfill{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventBackfill,
				PortalKey: portal.PortalKey,
			},
			GetDataFunc: func(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.FetchMessagesResponse, error) {
				if len(conv.GetMessages()) == 0 {
					return &bridgev2.FetchMessagesResponse{}, nil
				}
				messages := make([]*waWeb.WebMessageInfo, len(conv.GetMessages()))
				for i, rawMsg := range conv.GetMessages() {
					messages[i] = rawMsg.Message
				}
				zerolog.Ctx(ctx).Debug().
					Int("message_count", len(messages)).
					Stringer("end_of_history_type", conv.GetEndOfHistoryTransferType()).
					Msg("Converting messages to bridge from on-demand history sync")
				resp, err := wa.convertHistorySyncMessages(ctx, portal, portalJID, messages, false)
				if err != nil {
					return nil, err
				}
				resp.HasMore = conv.GetEndOfHistoryTransferType() == waHistorySync.Conversation_COMPLETE_BUT_MORE_MESSAGES_REMAIN_ON_PRIMARY
				return resp, nil
			},
		})
	}
}

func (wa *WhatsAppClient) convertHistorySyncMessage(
	ctx context.Context, portal *bridgev2.Portal, info *types.MessageInfo, msg, rawMsg *waE2E.Message, isViewOnce bool, reactions []*waWeb.Reaction,
) (*bridgev2.BackfillMessage, *wadb.MediaRequest) {
	// New messages turn these into edits, but in backfill we only have the last version,
	// so no need to do the edit thing. Instead, just unwrap the message.
	if msg.GetAssociatedChildMessage().GetMessage() != nil {
		msg = msg.GetAssociatedChildMessage().GetMessage()
	}
	// TODO use proper intent
	intent := wa.Main.Bridge.Bot
	wrapped := &bridgev2.BackfillMessage{
		ConvertedMessage: wa.Main.MsgConv.ToMatrix(ctx, portal, wa.Client, intent, msg, rawMsg, info, isViewOnce, true, nil),
		Sender:           wa.makeEventSender(ctx, info.Sender),
		ID:               waid.MakeMessageID(info.Chat, info.Sender, info.ID),
		TxnID:            networkid.TransactionID(waid.MakeMessageID(info.Chat, info.Sender, info.ID)),
		Timestamp:        info.Timestamp,
		StreamOrder:      info.Timestamp.Unix(),
		Reactions:        make([]*bridgev2.BackfillReaction, 0, len(reactions)),
	}
	mediaReq := wa.processFailedMedia(ctx, portal.PortalKey, wrapped.ID, wrapped.ConvertedMessage, true)
	for _, reaction := range reactions {
		var sender types.JID
		if reaction.GetKey().GetFromMe() {
			sender = wa.JID
		} else if reaction.GetKey().GetParticipant() != "" {
			sender, _ = types.ParseJID(*reaction.Key.Participant)
		} else if info.Chat.Server == types.DefaultUserServer || info.Chat.Server == types.BotServer {
			sender = info.Chat
		}
		if sender.IsEmpty() {
			continue
		}
		wrapped.Reactions = append(wrapped.Reactions, &bridgev2.BackfillReaction{
			TargetPart: ptr.Ptr(networkid.PartID("")),
			Timestamp:  time.UnixMilli(reaction.GetSenderTimestampMS()),
			Sender:     wa.makeEventSender(ctx, sender),
			Emoji:      reaction.GetText(),
		})
	}
	return wrapped, mediaReq
}
