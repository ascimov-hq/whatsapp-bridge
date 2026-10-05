package connector

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// Load-test L6-1: after a bridge restart, a login whose history sync portals still needed creating sat idle for
// history_sync.dispatch_wait (2 min) instead of the intended short resume delay, because the start-up wakeup of
// ManualHistorySyncDownload reset the timer. These run the real historySyncLoop against the connector's tables.

// startResumedHistorySyncLoop gives the login a whatsmeow client like a restarted bridge has (manual history sync
// download, nothing queued), marks its portals as still needing creation and starts the loop.
func startResumedHistorySyncLoop(t *testing.T, dispatchWait, resumeWait time.Duration) (*WhatsAppClient, func() bool) {
	t.Helper()
	origResume := historySyncResumeDispatchWait
	historySyncResumeDispatchWait = resumeWait
	t.Cleanup(func() { historySyncResumeDispatchWait = origResume })

	br, ul := newCatchUpTestBridge(t)
	wa := ul.Client.(*WhatsAppClient)
	wa.Client = whatsmeow.NewClient(store.NoopDevice, nil)
	wa.Client.ManualHistorySyncDownload = true
	wa.isNewLogin = false
	wa.Main.Config.HistorySync.DispatchWait = dispatchWait
	meta := ul.Metadata.(*waid.UserLoginMetadata)
	meta.LoggedInAt = jsontime.U(time.Now().Add(-time.Minute))
	meta.HistorySyncPortalsNeedCreating = true
	if err := ul.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go wa.historySyncLoop(ctx)
	return wa, func() bool { return !savedPortalsFlag(t, br, ul) }
}

// A resumed login with dispatch_wait = 1 min must dispatch after the resume delay. Before the fix the start-up
// wakeup turned the 5 s timer into dispatch_wait, so this timed out.
func TestResumedHistorySyncDispatchesWithoutDispatchWait(t *testing.T) {
	_, dispatched := startResumedHistorySyncLoop(t, time.Minute, 200*time.Millisecond)
	start := time.Now()
	waitForCondition(t, "the resumed login's history sync portals to be dispatched", dispatched)
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("dispatch took %s, expected about the resume delay", took)
	}
}

// The resume delay doesn't make the dispatch immediate: nothing happens before it, so a restart still gives the
// queued notifications and the connection a moment first.
func TestResumedHistorySyncWaitsForResumeDelay(t *testing.T) {
	_, dispatched := startResumedHistorySyncLoop(t, time.Minute, 1500*time.Millisecond)
	time.Sleep(700 * time.Millisecond)
	if dispatched() {
		t.Fatal("dispatched before the resume delay")
	}
	waitForCondition(t, "the resumed login's history sync portals to be dispatched", dispatched)
}

func TestHistorySyncDispatchDelay(t *testing.T) {
	origResume := historySyncResumeDispatchWait
	historySyncResumeDispatchWait = 5 * time.Second
	t.Cleanup(func() { historySyncResumeDispatchWait = origResume })
	const dispatchWait = 2 * time.Minute

	// New login: every history chunk waits dispatch_wait for more chunks (upstream behaviour).
	d := newHistorySyncDispatchDelay(dispatchWait, false)
	d.stored(true)
	if d.wait != dispatchWait {
		t.Errorf("new login: wait %s, expected %s", d.wait, dispatchWait)
	}

	// Resumed login: the start-up wakeup finds the queue empty, the short delay stays.
	d = newHistorySyncDispatchDelay(dispatchWait, true)
	d.drained()
	if d.wait != 5*time.Second {
		t.Errorf("resumed login, empty queue: wait %s, expected 5s", d.wait)
	}

	// Resumed login with a backlog of notifications queued before the restart: draining it keeps the short delay.
	d = newHistorySyncDispatchDelay(dispatchWait, true)
	d.stored(true)
	d.stored(false)
	d.stored(true)
	if d.wait != 5*time.Second {
		t.Errorf("resumed login, backlog: wait %s, expected 5s", d.wait)
	}
	d.drained()
	// History that arrives after the backlog was drained: the phone is still sending, wait for more chunks.
	d.stored(true)
	if d.wait != dispatchWait {
		t.Errorf("resumed login, new chunk after the backlog: wait %s, expected %s", d.wait, dispatchWait)
	}

	// A dispatch_wait shorter than the resume delay is never made longer.
	d = newHistorySyncDispatchDelay(time.Second, true)
	if d.wait != time.Second {
		t.Errorf("short dispatch_wait: wait %s, expected 1s", d.wait)
	}
}
