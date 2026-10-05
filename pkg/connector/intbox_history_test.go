package connector

import (
	"context"
	"reflect"
	"testing"

	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

func TestIntboxHistoryBypassLimit(t *testing.T) {
	for _, tc := range []struct{ maxInitial, sent, expected int }{
		{1000, 200, 800},
		{1000, 37, 963},
		{1000, 1000, 0},
		// The same-timestamp cutoff can send one more message than requested
		{200, 201, 0},
		{100, 200, 0},
	} {
		if limit := intboxHistoryBypassLimit(tc.maxInitial, tc.sent); limit != tc.expected {
			t.Errorf("intboxHistoryBypassLimit(%d, %d) = %d, expected %d", tc.maxInitial, tc.sent, limit, tc.expected)
		}
	}
}

// TestIntboxWrapHistoryBypass checks that only the initial forward backfill of a chat moves history into the
// bypass table, so that catch-up and backwards backfills can't push a chat over backfill.max_initial_messages.
func TestIntboxWrapHistoryBypass(t *testing.T) {
	wa := &WhatsAppClient{Main: &WhatsAppConnector{}}
	chatJID := types.NewJID("123", types.GroupServer)
	anchor := &database.Message{}
	for _, tc := range []struct {
		name     string
		enabled  bool
		params   bridgev2.FetchMessagesParams
		oldestTS uint64
		wrapped  bool
	}{
		{"initial forward", true, bridgev2.FetchMessagesParams{Forward: true}, 100, true},
		{"bypass disabled", false, bridgev2.FetchMessagesParams{Forward: true}, 100, false},
		{"catch-up forward", true, bridgev2.FetchMessagesParams{Forward: true, AnchorMessage: anchor}, 100, false},
		{"backwards", true, bridgev2.FetchMessagesParams{Forward: false}, 100, false},
		{"no timestamp", true, bridgev2.FetchMessagesParams{Forward: true}, 0, false},
	} {
		wa.Main.Config.Intbox.HistoryBypass.Enabled = tc.enabled
		orig := func() {}
		resp := &bridgev2.FetchMessagesResponse{CompleteCallback: orig}
		wa.intboxWrapHistoryBypass(context.Background(), tc.params, chatJID, tc.oldestTS, resp)
		wrapped := resp.CompleteCallback != nil &&
			reflect.ValueOf(resp.CompleteCallback).Pointer() != reflect.ValueOf(orig).Pointer()
		if wrapped != tc.wrapped {
			t.Errorf("%s: wrapped=%t, expected %t", tc.name, wrapped, tc.wrapped)
		}
	}
}
