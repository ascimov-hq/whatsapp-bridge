package connector

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

// testPairingClock gives logins admitted with admitAsync increasing pairing times (from 1001), so that the queue,
// which is ordered by pairing time, keeps the order in which the test admits them.
var testPairingClock atomic.Int64

func admitAsync(adm *intboxAdmission, ctx context.Context, cfg *IntboxConfig, loginID string, rooms int) chan *intboxImportRound {
	ch := make(chan *intboxImportRound, 1)
	pairedAt := 1000 + testPairingClock.Add(1)
	go func() {
		round, ok := adm.admit(ctx, cfg, networkid.UserLoginID(loginID), pairedAt, pairedAt, rooms)
		if !ok {
			round = nil
		}
		ch <- round
	}()
	return ch
}

func waitAdmitted(t *testing.T, ch chan *intboxImportRound, expectAdmitted bool) *intboxImportRound {
	t.Helper()
	select {
	case round := <-ch:
		if !expectAdmitted {
			t.Fatalf("import was admitted unexpectedly")
		}
		return round
	case <-time.After(200 * time.Millisecond):
		if expectAdmitted {
			t.Fatalf("import wasn't admitted")
		}
		return nil
	}
}

func TestIntboxAdmissionQueue(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 1
	cfg.Admission.MinRoomsInFlight = 10
	ctx := context.Background()

	roundA := waitAdmitted(t, admitAsync(&adm, ctx, cfg, "a", 20), true)
	chB := admitAsync(&adm, ctx, cfg, "b", 5)
	waitAdmitted(t, chB, false)
	if status := adm.status("b"); status.State != intboxImportQueued || status.QueuePosition != 1 {
		t.Fatalf("unexpected status for queued login: %+v", status)
	}
	chC := admitAsync(&adm, ctx, cfg, "c", 50)
	waitAdmitted(t, chC, false)
	if status := adm.status("c"); status.QueuePosition != 2 {
		t.Fatalf("unexpected queue position for second queued login: %+v", status)
	}
	// 10 rooms of A are still in flight, nobody is admitted yet
	for i := 0; i < 10; i++ {
		roundA.Done()
	}
	waitAdmitted(t, chB, false)
	// Fewer than min_rooms_in_flight rooms are left, so B is admitted early
	roundA.Done()
	roundB := waitAdmitted(t, chB, true)
	// A's 9 + B's 5 rooms are in flight and two imports are running, so C keeps waiting
	waitAdmitted(t, chC, false)
	for i := 0; i < 5; i++ {
		roundB.Done()
	}
	if status := adm.status("b"); status.State != intboxImportDone || status.RoomsDone != 5 {
		t.Fatalf("unexpected status for finished login: %+v", status)
	}
	// Only A's 9 rooms are in flight now
	waitAdmitted(t, chC, true)
	if status := adm.status("a"); status.State != intboxImportImporting || status.RoomsDone != 11 || status.RoomsTotal != 20 {
		t.Fatalf("unexpected status for importing login: %+v", status)
	}
	if status := adm.status("unknown"); status.State != intboxImportIdle {
		t.Fatalf("unexpected status for unknown login: %+v", status)
	}
}

func TestIntboxAdmissionCancel(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 1
	ctx := context.Background()

	waitAdmitted(t, admitAsync(&adm, ctx, cfg, "a", 3), true)
	cancelCtx, cancel := context.WithCancel(ctx)
	chB := admitAsync(&adm, cancelCtx, cfg, "b", 3)
	chC := admitAsync(&adm, ctx, cfg, "c", 3)
	waitAdmitted(t, chB, false)
	cancel()
	if round := <-chB; round != nil {
		t.Fatalf("cancelled import was admitted")
	}
	if status := adm.status("c"); status.QueuePosition != 1 {
		t.Fatalf("queue position didn't move after cancellation: %+v", status)
	}
	waitAdmitted(t, chC, false)
}

func TestIntboxAdmissionDisabled(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	ctx := context.Background()
	roundA := waitAdmitted(t, admitAsync(&adm, ctx, cfg, "a", 2), true)
	waitAdmitted(t, admitAsync(&adm, ctx, cfg, "b", 2), true)
	roundA.Done()
	roundA.Done()
	roundA.Wait()
	if status := adm.status("a"); status.State != intboxImportDone {
		t.Fatalf("unexpected status: %+v", status)
	}
	empty := waitAdmitted(t, admitAsync(&adm, ctx, cfg, "c", 0), true)
	empty.Wait()
	if status := adm.status("c"); status.State != intboxImportIdle {
		t.Fatalf("empty round changed state: %+v", status)
	}
}

// waitQueued waits until the login is in the admission queue, so that tests can rely on FIFO order.
func waitQueued(t *testing.T, adm *intboxAdmission, loginID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for adm.status(networkid.UserLoginID(loginID)).State != intboxImportQueued {
		if time.Now().After(deadline) {
			t.Fatalf("login %s wasn't queued", loginID)
		}
		time.Sleep(time.Millisecond)
	}
}

func holdingCount(adm *intboxAdmission) int {
	adm.lock.Lock()
	defer adm.lock.Unlock()
	n := 0
	for _, imp := range adm.imports {
		if imp.holding {
			n++
		}
	}
	return n
}

// The production setting (6 concurrent imports, min_rooms_in_flight 10) with the load-test arrival pattern: logins
// with ~100 rooms each arrive faster than they finish. The first six import, the rest wait in arrival order with a
// visible position, and a finished import hands its slot to the oldest waiting login.
func TestIntboxAdmissionSixSlotsFIFO(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 6
	cfg.Admission.MinRoomsInFlight = 10
	ctx := context.Background()

	rounds := map[string]*intboxImportRound{}
	chans := map[string]chan *intboxImportRound{}
	ids := []string{"l01", "l02", "l03", "l04", "l05", "l06", "l07", "l08", "l09", "l10"}
	for i, id := range ids {
		chans[id] = admitAsync(&adm, ctx, cfg, id, 100)
		if i < 6 {
			rounds[id] = waitAdmitted(t, chans[id], true)
		} else {
			waitQueued(t, &adm, id)
		}
	}
	if n := holdingCount(&adm); n != 6 {
		t.Fatalf("expected 6 imports holding a slot, got %d", n)
	}
	for i, id := range ids[6:] {
		if status := adm.status(networkid.UserLoginID(id)); status.QueuePosition != i+1 || status.ETASeconds <= 0 {
			t.Fatalf("unexpected status for queued login %s: %+v", id, status)
		}
	}
	// Half of the third login's rooms are done: nobody moves yet (600 - 50 rooms in flight >= 10)
	for i := 0; i < 50; i++ {
		rounds["l03"].Done()
	}
	waitAdmitted(t, chans["l07"], false)
	for i := 0; i < 50; i++ {
		rounds["l03"].Done()
	}
	// l03 finished, so the oldest waiting login gets its slot and everyone behind moves up by one
	rounds["l07"] = waitAdmitted(t, chans["l07"], true)
	waitAdmitted(t, chans["l08"], false)
	if status := adm.status("l08"); status.QueuePosition != 1 {
		t.Fatalf("queue didn't move up: %+v", status)
	}
	if status := adm.status("l10"); status.QueuePosition != 3 {
		t.Fatalf("queue didn't move up: %+v", status)
	}
	if n := holdingCount(&adm); n != 6 {
		t.Fatalf("expected 6 imports holding a slot, got %d", n)
	}
}

func TestIntboxAdmissionQueueETA(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 3
	ctx := context.Background()

	roundA := waitAdmitted(t, admitAsync(&adm, ctx, cfg, "a", 10), true)
	waitAdmitted(t, admitAsync(&adm, ctx, cfg, "b", 10), true)
	waitAdmitted(t, admitAsync(&adm, ctx, cfg, "c", 10), true)
	for _, id := range []string{"d", "e", "f", "g"} {
		admitAsync(&adm, ctx, cfg, id, 10)
		waitQueued(t, &adm, id)
	}
	adm.lock.Lock()
	adm.avgDuration = 10 * time.Minute
	adm.lock.Unlock()
	// No admitted import has finished a room yet, so each is expected to take the average 10 minutes.
	// Three slots: the first three in line start in ~10 minutes, the fourth when the first of them is done.
	// (The old estimate, position x average, said 10, 20, 30 and 40 minutes.)
	expect := map[string]int{"d": 600, "e": 600, "f": 600, "g": 1200}
	for id, eta := range expect {
		if status := adm.status(networkid.UserLoginID(id)); status.ETASeconds != eta {
			t.Fatalf("unexpected ETA for %s: got %d, expected %d (%+v)", id, status.ETASeconds, eta, status)
		}
	}
	// a is half done after 5 minutes, so its slot frees up in about 5 minutes
	for i := 0; i < 5; i++ {
		roundA.Done()
	}
	adm.lock.Lock()
	adm.imports["a"].startedAt = time.Now().Add(-5 * time.Minute)
	adm.lock.Unlock()
	if status := adm.status("d"); status.ETASeconds < 299 || status.ETASeconds > 300 {
		t.Fatalf("unexpected ETA for the first in line: %+v", status)
	}
	if status := adm.status("a"); status.ETASeconds < 299 || status.ETASeconds > 300 {
		t.Fatalf("unexpected ETA for the half-done import: %+v", status)
	}
	// One slot: the queue moves one import at a time
	var single intboxAdmission
	cfg1 := &IntboxConfig{}
	cfg1.Admission.Enabled = true
	cfg1.Admission.MaxConcurrentImports = 1
	waitAdmitted(t, admitAsync(&single, ctx, cfg1, "x", 3), true)
	for _, id := range []string{"y", "z"} {
		admitAsync(&single, ctx, cfg1, id, 3)
		waitQueued(t, &single, id)
	}
	single.lock.Lock()
	single.avgDuration = 4 * time.Minute
	single.lock.Unlock()
	if y, z := single.status("y"), single.status("z"); y.ETASeconds != 240 || z.ETASeconds != 480 {
		t.Fatalf("unexpected ETAs with one slot: %+v %+v", y, z)
	}
}

func admitPairing(adm *intboxAdmission, ctx context.Context, cfg *IntboxConfig, loginID string, loginTS, epoch int64, rooms int) chan *intboxImportRound {
	ch := make(chan *intboxImportRound, 1)
	go func() {
		round, ok := adm.admit(ctx, cfg, networkid.UserLoginID(loginID), loginTS, epoch, rooms)
		if !ok {
			round = nil
		}
		ch <- round
	}()
	return ch
}

func queueIDs(adm *intboxAdmission) []string {
	adm.lock.Lock()
	defer adm.lock.Unlock()
	ids := make([]string, len(adm.queue))
	for i, imp := range adm.queue {
		ids[i] = string(imp.loginID)
	}
	return ids
}

func storm(t *testing.T, adm *intboxAdmission, ctx context.Context, cfg *IntboxConfig, ids ...string) map[string]chan *intboxImportRound {
	t.Helper()
	chans := map[string]chan *intboxImportRound{}
	for _, id := range ids {
		chans[id] = admitAsync(adm, ctx, cfg, id, 4)
		waitQueued(t, adm, id)
	}
	return chans
}

// L4-3/L4-4: device_removed in the middle of an admitted import, then a re-login with ?login_id within
// reimport_after (same import epoch, new LoggedInAt). The retired instance's round is aborted, which hands its slot to
// the next login in the queue. The re-paired login used to go to the back of the queue (behind a whole login storm);
// it now continues at once, with its progress kept.
func TestIntboxAdmissionRepairMidImportContinues(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 1
	ctx := context.Background()

	oldCtx, retire := context.WithCancel(ctx)
	roundA := waitAdmitted(t, admitPairing(&adm, oldCtx, cfg, "a", 100, 100, 10), true)
	before := adm.status("a")
	for i := 0; i < 4; i++ {
		roundA.Done()
	}
	waiting := storm(t, &adm, ctx, cfg, "s1", "s2", "s3")
	retire()
	// The aborted import's slot goes to the oldest waiting login, as before
	waitAdmitted(t, waiting["s1"], true)
	deadline := time.Now().Add(2 * time.Second)
	for adm.status("a").State != intboxImportIdle {
		if time.Now().After(deadline) {
			t.Fatalf("retired import wasn't aborted: %+v", adm.status("a"))
		}
		time.Sleep(time.Millisecond)
	}
	// The re-pair (LoggedInAt 160, epoch kept): the 6 chats without a room yet are new rooms
	roundA2 := waitAdmitted(t, admitPairing(&adm, ctx, cfg, "a", 160, 100, 6), true)
	status := adm.status("a")
	if status.State != intboxImportImporting || status.QueuePosition != 0 || status.RoomsDone != 4 ||
		status.RoomsTotal != 10 || status.StartedAt != before.StartedAt {
		t.Fatalf("re-paired import didn't continue: %+v (before %+v)", status, before)
	}
	if ids := queueIDs(&adm); len(ids) != 2 || ids[0] != "s2" || ids[1] != "s3" {
		t.Fatalf("queue changed: %v", ids)
	}
	for i := 0; i < 6; i++ {
		roundA2.Done()
	}
	roundA2.Wait()
	if status = adm.status("a"); status.State != intboxImportDone || status.RoomsDone != 10 {
		t.Fatalf("unexpected status after the re-paired import finished: %+v", status)
	}
	// s1 still holds the only slot
	waitAdmitted(t, waiting["s2"], false)
}

// L4-3: a re-pair of a finished import within the epoch with one chat that has no room yet (e.g. a chat started
// between the two pairings) used to queue behind the whole storm for that one room.
func TestIntboxAdmissionRepairAfterDoneSkipsQueue(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 1
	ctx := context.Background()

	roundA := waitAdmitted(t, admitPairing(&adm, ctx, cfg, "a", 100, 100, 2), true)
	roundA.Done()
	roundA.Done()
	waitAdmitted(t, admitAsync(&adm, ctx, cfg, "s1", 4), true)
	waiting := storm(t, &adm, ctx, cfg, "s2", "s3")
	roundA2 := waitAdmitted(t, admitPairing(&adm, ctx, cfg, "a", 160, 100, 1), true)
	if status := adm.status("a"); status.State != intboxImportImporting || status.RoomsTotal != 3 || status.RoomsDone != 2 {
		t.Fatalf("unexpected status: %+v", status)
	}
	roundA2.Done()
	if status := adm.status("a"); status.State != intboxImportDone {
		t.Fatalf("unexpected status: %+v", status)
	}
	waitAdmitted(t, waiting["s2"], false)
}

// A re-pair of a login that was still waiting keeps its place in the queue, whether the retired instance's wait was
// aborted before the new pairing asked for admission or not.
func TestIntboxAdmissionRepairWhileQueuedKeepsPlace(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 1
	ctx := context.Background()

	roundX := waitAdmitted(t, admitAsync(&adm, ctx, cfg, "x", 1), true)
	oldCtxA, retireA := context.WithCancel(ctx)
	chA := admitPairing(&adm, oldCtxA, cfg, "a", 100, 100, 5)
	waitQueued(t, &adm, "a")
	oldCtxB, retireB := context.WithCancel(ctx)
	chB := admitPairing(&adm, oldCtxB, cfg, "b", 100, 100, 5)
	waitQueued(t, &adm, "b")
	waiting := storm(t, &adm, ctx, cfg, "s1", "s2")

	// a: the old wait is aborted first (removed from the queue), then the re-pair asks for admission
	retireA()
	if round := <-chA; round != nil {
		t.Fatalf("retired wait was admitted")
	}
	chA2 := admitPairing(&adm, ctx, cfg, "a", 160, 100, 5)
	// b: the re-pair asks for admission before the old wait is aborted
	chB2 := admitPairing(&adm, ctx, cfg, "b", 170, 100, 5)
	deadline := time.Now().Add(2 * time.Second)
	for {
		ids := queueIDs(&adm)
		if len(ids) == 4 && ids[0] == "a" && ids[1] == "b" && ids[2] == "s1" && ids[3] == "s2" &&
			adm.status("b").State == intboxImportQueued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("re-paired logins didn't keep their places: %v", ids)
		}
		time.Sleep(time.Millisecond)
	}
	retireB()
	if round := <-chB; round != nil {
		t.Fatalf("retired wait was admitted")
	}
	if status := adm.status("a"); status.QueuePosition != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if status := adm.status("b"); status.QueuePosition != 2 {
		t.Fatalf("unexpected status: %+v", status)
	}
	roundX.Done()
	roundA := waitAdmitted(t, chA2, true)
	waitAdmitted(t, chB2, false)
	for i := 0; i < 5; i++ {
		roundA.Done()
	}
	waitAdmitted(t, chB2, true)
	waitAdmitted(t, waiting["s1"], false)
}

// A pairing after reimport_after starts a new epoch, which is a new import: it queues like any other login.
func TestIntboxAdmissionNewEpochQueues(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 1
	ctx := context.Background()

	roundA := waitAdmitted(t, admitPairing(&adm, ctx, cfg, "a", 100, 100, 1), true)
	roundA.Done()
	waitAdmitted(t, admitAsync(&adm, ctx, cfg, "s1", 4), true)
	storm(t, &adm, ctx, cfg, "s2")
	chA2 := admitPairing(&adm, ctx, cfg, "a", 900_000, 900_000, 3)
	waitAdmitted(t, chA2, false)
	if status := adm.status("a"); status.State != intboxImportQueued || status.QueuePosition != 2 || status.RoomsDone != 0 ||
		status.RoomsTotal != 3 {
		t.Fatalf("new epoch didn't queue as a new import: %+v", status)
	}
}

func admitSizedAsync(
	adm *intboxAdmission, ctx context.Context, cfg *IntboxConfig, loginID string, pairedAt int64, rooms, messages int,
) chan *intboxImportRound {
	ch := make(chan *intboxImportRound, 1)
	go func() {
		round, ok := adm.admitSized(ctx, cfg, networkid.UserLoginID(loginID), pairedAt, pairedAt, rooms, messages)
		if !ok {
			round = nil
		}
		ch <- round
	}()
	return ch
}

// l7Config is the deployed admission config (homeserver/configs/whatsapp.yaml) after L7-1.
func l7Config() *IntboxConfig {
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 5
	cfg.Admission.MinRoomsInFlight = 10
	cfg.Admission.SmallImports.MaxRooms = 10
	cfg.Admission.SmallImports.MaxMessages = 1000
	cfg.Admission.SmallImports.MaxConcurrent = 10
	cfg.Admission.HeavyImports.MinMessages = 40000
	cfg.Admission.HeavyImports.MaxConcurrent = 1
	return cfg
}

func TestIntboxClassifyImport(t *testing.T) {
	cfg := l7Config()
	for _, tc := range []struct {
		rooms, messages int
		small, heavy    bool
	}{
		{5, 100, true, false},      // tiny storm account (sim profile 04)
		{10, 1000, true, false},    // at both limits
		{11, 100, false, false},    // too many rooms
		{5, 1001, false, false},    // few rooms, but a lot of history
		{5, -1, false, false},      // unknown size: never small when max_messages is set
		{100, 9200, false, false},  // typical (profile 01)
		{100, 26000, false, false}, // heavy p90 (profile 02)
		{100, 40000, false, true},  // at the heavy limit
		{100, 118000, false, true}, // C1-max (profile 08)
		{100, -1, false, false},    // unknown size: main lane
	} {
		if small, heavy := classifyImport(cfg, tc.rooms, tc.messages); small != tc.small || heavy != tc.heavy {
			t.Errorf("classifyImport(%d rooms, %d messages) = small %v heavy %v", tc.rooms, tc.messages, small, heavy)
		}
	}
	if limit := intboxImportMessageCountLimit(cfg); limit != 40001 {
		t.Errorf("unexpected count limit %d", limit)
	}
	// Lanes off (the defaults): nothing is counted or classified, as before
	off := &IntboxConfig{}
	off.Admission.Enabled = true
	off.Admission.MaxConcurrentImports = 5
	if small, heavy := classifyImport(off, 1, 1); small || heavy || intboxImportMessageCountLimit(off) != 0 {
		t.Errorf("lanes should be off by default")
	}
	// Rooms only
	off.Admission.SmallImports.MaxRooms = 5
	off.Admission.SmallImports.MaxConcurrent = 2
	if small, _ := classifyImport(off, 5, -1); !small || intboxImportMessageCountLimit(off) != 0 {
		t.Errorf("max_messages 0 should classify by rooms only without counting")
	}
	off.Admission.Enabled = false
	if small, _ := classifyImport(off, 5, -1); small {
		t.Errorf("disabled admission shouldn't classify")
	}
}

// L7-1, replayed from the L7 bridge log (5 Oct, 06:12-06:15): 2 C1-max and 6 typical accounts paired along with a storm
// of tiny logins (06:12:04-06:12:35), but the tiny accounts' history was dispatched first, so the typical and C1-max
// imports waited at positions 27-34 behind ~30 tiny ones, and later behind tiny logins that held a slot for minutes.
// Now the tiny ones have their own lane, the main queue is in pairing order, and only one C1-max imports at a time.
func TestIntboxAdmissionL7StormLanes(t *testing.T) {
	var adm intboxAdmission
	cfg := l7Config()
	ctx := context.Background()

	// 30 tiny logins (5 rooms, 100 messages) are dispatched first: 10 import in the small lane, 20 wait there
	var tiny []string
	tinyRounds := map[string]*intboxImportRound{}
	tinyChans := map[string]chan *intboxImportRound{}
	for i := 0; i < 30; i++ {
		id := "tiny" + string(rune('A'+i))
		tiny = append(tiny, id)
		tinyChans[id] = admitSizedAsync(&adm, ctx, cfg, id, 4+int64(i/4), 5, 100)
		if i < 10 {
			tinyRounds[id] = waitAdmitted(t, tinyChans[id], true)
		} else {
			waitQueued(t, &adm, id)
		}
	}
	// Then the big ones, in the log's dispatch order, with their pairing times (seconds after 06:12:00)
	type login struct {
		id       string
		pairedAt int64
		messages int
	}
	big := []login{
		{"t1", 6, 9200}, {"t2", 10, 9200}, {"t3", 15, 9200}, {"t4", 22, 9200}, {"t5", 29, 9200}, {"t6", 33, 9200},
		{"c1a", 5, 118000}, {"c1b", 10, 118000},
	}
	rounds := map[string]*intboxImportRound{}
	chans := map[string]chan *intboxImportRound{}
	for i, l := range big {
		chans[l.id] = admitSizedAsync(&adm, ctx, cfg, l.id, l.pairedAt, 100, l.messages)
		if i < 5 {
			// The tiny logins don't take main slots: the first five big imports start at once
			rounds[l.id] = waitAdmitted(t, chans[l.id], true)
		} else {
			waitQueued(t, &adm, l.id)
		}
	}
	// The main queue is in pairing order (c1a 06:12:05, c1b 06:12:10, t6 06:12:33), and positions are per lane
	for id, position := range map[string]int{"c1a": 1, "c1b": 2, "t6": 3} {
		if s := adm.status(networkid.UserLoginID(id)); s.State != intboxImportQueued || s.QueuePosition != position || s.ETASeconds <= 0 {
			t.Fatalf("unexpected status for %s: %+v", id, s)
		}
	}
	if s := adm.status(networkid.UserLoginID(tiny[10])); s.State != intboxImportQueued || s.QueuePosition != 1 {
		t.Fatalf("unexpected status for the first waiting tiny login: %+v", s)
	}
	// A tiny import finishing hands its slot to the next tiny login, never to a main-lane one
	for i := 0; i < 5; i++ {
		tinyRounds[tiny[0]].Done()
	}
	waitAdmitted(t, tinyChans[tiny[10]], true)
	waitAdmitted(t, chans["c1a"], false)
	// t1 finishes: c1a (the oldest pairing in the main queue) gets its slot
	for i := 0; i < 100; i++ {
		rounds["t1"].Done()
	}
	rounds["c1a"] = waitAdmitted(t, chans["c1a"], true)
	// t2 finishes: c1b is heavy and c1a has the only heavy slot, so t6 passes it
	for i := 0; i < 100; i++ {
		rounds["t2"].Done()
	}
	waitAdmitted(t, chans["t6"], true)
	waitAdmitted(t, chans["c1b"], false)
	if s := adm.status("c1b"); s.State != intboxImportQueued || s.QueuePosition != 1 {
		t.Fatalf("unexpected status for c1b: %+v", s)
	}
	if n := holdingCount(&adm); n != 5+10 {
		t.Fatalf("expected 5 main and 10 small slot holders, got %d", n)
	}
	// c1a finishes: c1b gets the heavy slot
	for i := 0; i < 100; i++ {
		rounds["c1a"].Done()
	}
	waitAdmitted(t, chans["c1b"], true)
}

// Without the lanes (small_imports/heavy_imports unset, the old config), every import shares the main slots as before;
// only the order changed: a login that paired earlier but whose history was dispatched later is ahead in the queue.
func TestIntboxAdmissionPairingOrderWithoutLanes(t *testing.T) {
	var adm intboxAdmission
	cfg := &IntboxConfig{}
	cfg.Admission.Enabled = true
	cfg.Admission.MaxConcurrentImports = 1
	ctx := context.Background()

	waitAdmitted(t, admitSizedAsync(&adm, ctx, cfg, "first", 100, 5, 100), true)
	admitSizedAsync(&adm, ctx, cfg, "tiny", 120, 5, 100)
	waitQueued(t, &adm, "tiny")
	// Paired before "tiny", but its 7 history chunks took longer, so it asks for admission later
	admitSizedAsync(&adm, ctx, cfg, "typical", 110, 100, 9200)
	waitQueued(t, &adm, "typical")
	// Same pairing second: arrival order
	admitSizedAsync(&adm, ctx, cfg, "same", 110, 100, 9200)
	waitQueued(t, &adm, "same")
	if ids := queueIDs(&adm); len(ids) != 3 || ids[0] != "typical" || ids[1] != "same" || ids[2] != "tiny" {
		t.Fatalf("queue isn't in pairing order: %v", ids)
	}
	if s := adm.status("tiny"); s.QueuePosition != 3 {
		t.Fatalf("unexpected status: %+v", s)
	}
}

// A later history sync round of a finished small import that isn't small itself waits in the main lane.
func TestIntboxAdmissionSmallImportGrows(t *testing.T) {
	var adm intboxAdmission
	cfg := l7Config()
	cfg.Admission.MaxConcurrentImports = 1
	ctx := context.Background()

	roundBig := waitAdmitted(t, admitSizedAsync(&adm, ctx, cfg, "big", 100, 100, 9200), true)
	round := waitAdmitted(t, admitSizedAsync(&adm, ctx, cfg, "a", 110, 3, 60), true)
	for i := 0; i < 3; i++ {
		round.Done()
	}
	if s := adm.status("a"); s.State != intboxImportDone {
		t.Fatalf("unexpected status: %+v", s)
	}
	// Another small round goes straight on, like any later round of an admitted login
	small := waitAdmitted(t, admitSizedAsync(&adm, ctx, cfg, "a", 110, 2, 40), true)
	small.Done()
	small.Done()
	// A big round waits for the main slot, held by "big"
	chA := admitSizedAsync(&adm, ctx, cfg, "a", 110, 80, 5000)
	waitQueued(t, &adm, "a")
	if s := adm.status("a"); s.QueuePosition != 1 || s.RoomsTotal != 85 || s.RoomsDone != 5 {
		t.Fatalf("unexpected status: %+v", s)
	}
	for i := 0; i < 100; i++ {
		roundBig.Done()
	}
	waitAdmitted(t, chA, true)
}
