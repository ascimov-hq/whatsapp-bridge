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
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// W4: admission queue for history sync imports.
//
// Every login's history sync portal creation (getting the chat info, creating the room and running
// the initial forward backfill) is tracked as an "import". When admission is enabled, imports wait
// in a bridge-wide FIFO queue until either fewer than max_concurrent_imports logins are importing,
// or fewer than min_rooms_in_flight rooms are still importing across all admitted logins (so that the
// tail of a big import doesn't leave the homeserver idle, and so that there are always enough rooms
// importing at once for the per-room message rate to stay low). The weight of an admitted import is
// the number of its rooms that are still in flight.
//
// The same tracking is used for the onboarding status endpoint even when admission is disabled.
//
// L7-1 (load test "everything at once"): the queue used to be ordered by when the history sync was dispatched
// (dispatch_wait after the last history chunk), and every import took one of the same slots. Typical accounts that
// paired along with a login storm queued behind ~30 tiny storm logins (their history arrives in fewer chunks, so it's
// dispatched sooner), and then behind tiny logins that held a slot for minutes while sharing the paced rate with the
// big imports. Now:
//   - the queue is ordered by pairing time (LoggedInAt, carried over by a continued import), then arrival;
//   - small imports (intbox.admission.small_imports) have a lane of their own with separate slots;
//   - heavy imports (intbox.admission.heavy_imports) may only hold some of the main slots, and a normal import
//     behind a heavy one that waits for a heavy slot may pass it.
//
// Queue positions and ETAs are per lane.

const (
	intboxImportQueued    = "queued"
	intboxImportImporting = "importing"
	intboxImportDone      = "done"
	intboxImportIdle      = "idle"

	intboxDefaultImportDuration = 10 * time.Minute
	// An admitted import that hasn't finished a single room in this long stops counting against the limits.
	intboxImportStallTimeout = 30 * time.Minute
)

type intboxAdmission struct {
	lock        sync.Mutex
	cfg         *IntboxConfig
	queue       []*intboxImport
	imports     map[networkid.UserLoginID]*intboxImport
	avgDuration time.Duration
	// Average duration of imports in the small lane (they would make the main lane's ETAs far too short)
	avgSmallDuration time.Duration
	stallOnce        sync.Once
}

type intboxImport struct {
	loginID networkid.UserLoginID
	loginTS int64
	// The account's import epoch (UserLoginMetadata.GetImportEpoch): the same across re-pairs within
	// intbox.reimport_after, so a re-pair continues the import instead of queueing as a new one.
	epoch int64
	// Queue order: the pairing time (loginTS), or the earlier import's when it's continued. Ties by queuedAt.
	arrival int64
	// The import runs in the small-import lane (intbox.admission.small_imports)
	small bool
	// The import counts against intbox.admission.heavy_imports.max_concurrent
	heavy bool

	state    string
	holding  bool
	admitted chan struct{}

	roomsTotal   int
	roomsDone    int
	pending      int
	messagesSent int

	queuedAt     time.Time
	startedAt    time.Time
	finishedAt   time.Time
	lastProgress time.Time
}

// intboxImportRound tracks the rooms of one createPortalsFromHistorySync call.
// It's used in place of a sync.WaitGroup: Done is called once per room and Wait blocks until all rooms are done.
type intboxImportRound struct {
	adm     *intboxAdmission
	imp     *intboxImport
	pending int
	done    chan struct{}
}

func (round *intboxImportRound) Done() {
	round.adm.roomDone(round)
}

func (round *intboxImportRound) Wait() {
	<-round.done
}

// DoneChan returns a channel that is closed when all rooms of the round are done.
func (round *intboxImportRound) DoneChan() <-chan struct{} {
	return round.done
}

// intboxAdmitImport registers a round of history sync portal creation for this login and waits until
// the admission queue lets it start. It returns false if the context was cancelled while waiting.
// messages is the number of buffered history sync messages of the login (see intboxImportMessageCountLimit),
// or -1 if it's unknown.
func (wa *WhatsAppClient) intboxAdmitImport(ctx context.Context, rooms, messages int) (*intboxImportRound, bool) {
	// Imports stay keyed by the pairing (LoggedInAt), not the import epoch: after a re-pair, the retired
	// instance's rounds are aborted when its loop stops, and that must not mark the new instance's import idle.
	// A re-pair within intbox.reimport_after only counts its new rooms (catch-ups aren't admitted), usually 0,
	// and it keeps the place the epoch's import already had (see continueImportLocked).
	meta := wa.UserLogin.Metadata.(*waid.UserLoginMetadata)
	loginTS := meta.LoggedInAt.Unix()
	epoch := meta.GetImportEpoch().Unix()
	return wa.Main.intbox.admission.admitSized(ctx, &wa.Main.Config.Intbox, wa.UserLogin.ID, loginTS, epoch, rooms, messages)
}

// intboxImportMessageCountLimit returns how many buffered history sync messages need to be counted to classify an
// import (small or heavy), plus one, or 0 if the lanes are disabled and nothing needs to be counted.
func intboxImportMessageCountLimit(cfg *IntboxConfig) int {
	adm := &cfg.Admission
	if !adm.Enabled {
		return 0
	}
	limit := 0
	if adm.SmallImports.MaxRooms > 0 && adm.SmallImports.MaxConcurrent > 0 {
		limit = adm.SmallImports.MaxMessages
	}
	if adm.HeavyImports.MinMessages > 0 && adm.HeavyImports.MaxConcurrent > 0 {
		limit = max(limit, adm.HeavyImports.MinMessages)
	}
	if limit <= 0 {
		return 0
	}
	return limit + 1
}

// classifyImport returns the lane of a history sync round with the given number of rooms and buffered messages
// (-1 if unknown): small (own lane), heavy (limited share of the main slots), or neither.
func classifyImport(cfg *IntboxConfig, rooms, messages int) (small, heavy bool) {
	adm := &cfg.Admission
	if !adm.Enabled {
		return false, false
	}
	if s := adm.SmallImports; s.MaxRooms > 0 && s.MaxConcurrent > 0 && rooms <= s.MaxRooms &&
		(s.MaxMessages <= 0 || (messages >= 0 && messages <= s.MaxMessages)) {
		return true, false
	}
	if h := adm.HeavyImports; h.MinMessages > 0 && h.MaxConcurrent > 0 && messages >= h.MinMessages {
		return false, true
	}
	return false, false
}

// continueImportLocked carries the admission of an earlier import of the same epoch over to a new one (a re-pair
// within intbox.reimport_after, e.g. after device_removed, or the same pairing reconnecting after its rounds were
// aborted). Before, the new import always went to the back of the queue: under a login storm a user who re-paired
// mid-import, or re-paired a finished import with a single new chat, waited for the whole queue again (L4-3).
//   - The earlier import had been admitted (importing, done, or aborted after it started): the new one starts at once,
//     like later history sync rounds of an admitted login. It may briefly exceed max_concurrent_imports, because the
//     aborted import's slot has usually been handed on already.
//   - It was still waiting (queued, or aborted while queued): the new one takes its place in the queue.
//
// Progress (rooms done, messages sent, start time) carries over, so the onboarding status doesn't restart from 0.
// Must be called with the lock held.
func (adm *intboxAdmission) continueImportLocked(prev, imp *intboxImport, now time.Time, enabled bool) {
	imp.roomsTotal = prev.roomsDone
	imp.roomsDone = prev.roomsDone
	imp.messagesSent = prev.messagesSent
	imp.arrival = prev.arrival
	imp.small = prev.small
	imp.heavy = prev.heavy
	if !prev.startedAt.IsZero() {
		imp.startedAt = prev.startedAt
		adm.startLocked(imp, now, enabled)
		return
	}
	imp.state = intboxImportQueued
	imp.queuedAt = prev.queuedAt
	if imp.queuedAt.IsZero() {
		imp.queuedAt = now
	}
	if idx := slices.Index(adm.queue, prev); idx >= 0 {
		adm.queue[idx] = imp
	} else {
		// Aborted while queued (removed from the queue): back to its place
		adm.enqueueLocked(imp)
	}
}

// enqueueLocked inserts the import into the queue by pairing time, then by the time it was queued.
// Must be called with the lock held.
func (adm *intboxAdmission) enqueueLocked(imp *intboxImport) {
	idx := len(adm.queue)
	for i, queued := range adm.queue {
		if queued.arrival > imp.arrival || (queued.arrival == imp.arrival && queued.queuedAt.After(imp.queuedAt)) {
			idx = i
			break
		}
	}
	adm.queue = slices.Insert(adm.queue, idx, imp)
}

func (adm *intboxAdmission) admit(
	ctx context.Context, cfg *IntboxConfig, loginID networkid.UserLoginID, loginTS, epoch int64, rooms int,
) (*intboxImportRound, bool) {
	return adm.admitSized(ctx, cfg, loginID, loginTS, epoch, rooms, -1)
}

func (adm *intboxAdmission) admitSized(
	ctx context.Context, cfg *IntboxConfig, loginID networkid.UserLoginID, loginTS, epoch int64, rooms, messages int,
) (*intboxImportRound, bool) {
	log := zerolog.Ctx(ctx)
	adm.lock.Lock()
	if adm.imports == nil {
		adm.imports = make(map[networkid.UserLoginID]*intboxImport)
		adm.avgDuration = intboxDefaultImportDuration
		adm.avgSmallDuration = intboxDefaultImportDuration
	}
	adm.cfg = cfg
	if cfg.Admission.Enabled {
		adm.stallOnce.Do(func() {
			go adm.stallLoop(context.WithoutCancel(ctx))
		})
	}
	round := &intboxImportRound{adm: adm, pending: rooms, done: make(chan struct{})}
	if rooms <= 0 {
		close(round.done)
		adm.lock.Unlock()
		return round, true
	}
	now := time.Now()
	imp := adm.imports[loginID]
	if imp == nil || imp.loginTS != loginTS || imp.state == intboxImportIdle {
		prev := imp
		imp = &intboxImport{
			loginID:  loginID,
			loginTS:  loginTS,
			epoch:    epoch,
			arrival:  loginTS,
			admitted: make(chan struct{}),
		}
		imp.small, imp.heavy = classifyImport(cfg, rooms, messages)
		if prev != nil && prev.epoch == epoch && cfg.Admission.Enabled {
			adm.continueImportLocked(prev, imp, now, true)
		}
		if prev != nil {
			adm.removeFromQueueLocked(prev)
			prev.holding = false
		}
		adm.imports[loginID] = imp
	}
	round.imp = imp
	if cfg.Admission.Enabled && imp.small && imp.state == intboxImportDone {
		if small, heavy := classifyImport(cfg, rooms, messages); !small {
			// A later history sync round of a finished small import that isn't small itself is real work:
			// it waits in the main lane instead of taking a small-import slot for a long time.
			imp.small, imp.heavy = false, heavy
			imp.state = ""
			imp.admitted = make(chan struct{})
		}
	}
	imp.roomsTotal += rooms
	imp.pending += rooms
	switch {
	case imp.state == intboxImportImporting || imp.state == intboxImportDone || !cfg.Admission.Enabled:
		// Logins that were already admitted (later history sync rounds) never go back to the queue.
		if imp.state != intboxImportImporting {
			adm.startLocked(imp, now, cfg.Admission.Enabled)
		}
		imp.lastProgress = now
		adm.lock.Unlock()
		go adm.watchRound(ctx, round)
		return round, true
	case imp.state == "":
		imp.state = intboxImportQueued
		imp.queuedAt = now
		adm.enqueueLocked(imp)
	}
	adm.dispatchLocked(cfg, now)
	position := adm.queuePositionLocked(imp)
	lane := imp.lane()
	adm.lock.Unlock()
	if position > 0 {
		log.Info().
			Int("queue_position", position).
			Int("rooms", rooms).
			Int("buffered_messages", messages).
			Str("lane", lane).
			Msg("History sync import is queued for admission")
	}
	select {
	case <-imp.admitted:
		go adm.watchRound(ctx, round)
		return round, true
	case <-ctx.Done():
		adm.abort(imp)
		return nil, false
	}
}

func (adm *intboxAdmission) startLocked(imp *intboxImport, now time.Time, holding bool) {
	imp.state = intboxImportImporting
	imp.holding = holding
	if imp.startedAt.IsZero() {
		imp.startedAt = now
	}
	imp.lastProgress = now
	select {
	case <-imp.admitted:
	default:
		close(imp.admitted)
	}
}

func (adm *intboxAdmission) removeFromQueueLocked(imp *intboxImport) {
	adm.queue = slices.DeleteFunc(adm.queue, func(queued *intboxImport) bool {
		return queued == imp
	})
}

func (imp *intboxImport) lane() string {
	switch {
	case imp.small:
		return "small"
	case imp.heavy:
		return "heavy"
	default:
		return "main"
	}
}

// dispatchLocked admits queued imports while there's capacity. Must be called with the lock held.
func (adm *intboxAdmission) dispatchLocked(cfg *IntboxConfig, now time.Time) {
	for {
		idx := adm.nextAdmissibleLocked(cfg)
		if idx < 0 {
			return
		}
		next := adm.queue[idx]
		adm.queue = slices.Delete(adm.queue, idx, idx+1)
		adm.startLocked(next, now, cfg.Admission.Enabled)
	}
}

// nextAdmissibleLocked returns the index of the first queued import that may start now, or -1.
//   - Main lane: fewer than max_concurrent_imports imports hold a main slot, or fewer than min_rooms_in_flight of
//     their rooms are still importing. Heavy imports also need fewer than heavy_imports.max_concurrent heavy holders;
//     a normal import may pass a heavy one that waits for that.
//   - Small lane: fewer than small_imports.max_concurrent small imports hold a slot.
//
// Must be called with the lock held.
func (adm *intboxAdmission) nextAdmissibleLocked(cfg *IntboxConfig) int {
	if len(adm.queue) == 0 {
		return -1
	} else if !cfg.Admission.Enabled {
		return 0
	}
	var holding, roomsInFlight, heavy, small int
	for _, imp := range adm.imports {
		if !imp.holding {
			continue
		} else if imp.small {
			small++
			continue
		}
		holding++
		roomsInFlight += imp.pending
		if imp.heavy {
			heavy++
		}
	}
	mainFree := holding < max(cfg.Admission.MaxConcurrentImports, 1) ||
		(cfg.Admission.MinRoomsInFlight > 0 && roomsInFlight < cfg.Admission.MinRoomsInFlight)
	heavyFree := cfg.Admission.HeavyImports.MaxConcurrent <= 0 || heavy < cfg.Admission.HeavyImports.MaxConcurrent
	smallFree := small < cfg.Admission.SmallImports.MaxConcurrent
	for i, imp := range adm.queue {
		switch {
		case imp.small:
			if smallFree {
				return i
			}
		case imp.heavy:
			if mainFree && heavyFree {
				return i
			}
		default:
			if mainFree {
				return i
			}
		}
	}
	return -1
}

// queuePositionLocked returns the 1-based position of the import among the queued imports of its lane (small or
// main), or 0 if it isn't queued. Must be called with the lock held.
func (adm *intboxAdmission) queuePositionLocked(imp *intboxImport) int {
	position := 0
	for _, queued := range adm.queue {
		if queued.small == imp.small {
			position++
		}
		if queued == imp {
			return position
		}
	}
	return 0
}

func (adm *intboxAdmission) roomDone(round *intboxImportRound) {
	adm.lock.Lock()
	defer adm.lock.Unlock()
	if round.pending <= 0 {
		return
	}
	round.pending--
	if round.pending == 0 {
		close(round.done)
	}
	imp := round.imp
	if imp == nil {
		return
	}
	now := time.Now()
	imp.pending = max(imp.pending-1, 0)
	imp.roomsDone++
	imp.lastProgress = now
	if imp.pending == 0 && imp.state == intboxImportImporting {
		imp.state = intboxImportDone
		imp.finishedAt = now
		if !imp.startedAt.IsZero() {
			// Exponential moving average of how long an admitted import takes, per lane, used for queue ETAs
			if imp.small {
				adm.avgSmallDuration = (adm.avgSmallDuration*7 + now.Sub(imp.startedAt)*3) / 10
			} else {
				adm.avgDuration = (adm.avgDuration*7 + now.Sub(imp.startedAt)*3) / 10
			}
		}
		imp.holding = false
	}
	if len(adm.queue) > 0 && adm.cfg != nil {
		adm.dispatchLocked(adm.cfg, now)
	}
}

// watchRound releases the admission slot if the history sync loop is stopped (e.g. the login disconnected)
// before all rooms of the round were processed.
func (adm *intboxAdmission) watchRound(ctx context.Context, round *intboxImportRound) {
	select {
	case <-round.done:
	case <-ctx.Done():
		adm.abort(round.imp)
	}
}

func (adm *intboxAdmission) abort(imp *intboxImport) {
	if imp == nil {
		return
	}
	adm.lock.Lock()
	defer adm.lock.Unlock()
	if imp.state == intboxImportDone {
		return
	}
	adm.removeFromQueueLocked(imp)
	imp.state = intboxImportIdle
	imp.holding = false
	if adm.cfg != nil {
		adm.dispatchLocked(adm.cfg, time.Now())
	}
}

// stallLoop stops counting admitted imports that haven't made progress in a long time against the limits,
// so a stuck portal can't block every other onboarding.
func (adm *intboxAdmission) stallLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
		adm.lock.Lock()
		now := time.Now()
		for _, imp := range adm.imports {
			if imp.holding && now.Sub(imp.lastProgress) > intboxImportStallTimeout {
				zerolog.Ctx(ctx).Warn().
					Str("login_id", string(imp.loginID)).
					Int("rooms_pending", imp.pending).
					Time("last_progress", imp.lastProgress).
					Msg("History sync import hasn't made progress, releasing its admission slot")
				imp.holding = false
			}
		}
		adm.dispatchLocked(adm.cfg, now)
		adm.lock.Unlock()
	}
}

func (adm *intboxAdmission) addMessages(loginID networkid.UserLoginID, count int) {
	if count <= 0 {
		return
	}
	adm.lock.Lock()
	defer adm.lock.Unlock()
	if imp := adm.imports[loginID]; imp != nil && imp.state == intboxImportImporting {
		imp.messagesSent += count
	}
}

type IntboxOnboardingLogin struct {
	LoginID       networkid.UserLoginID `json:"login_id"`
	State         string                `json:"state"`
	QueuePosition int                   `json:"queue_position"`
	RoomsTotal    int                   `json:"rooms_total"`
	RoomsDone     int                   `json:"rooms_done"`
	MessagesSent  int                   `json:"messages_sent"`
	StartedAt     int64                 `json:"started_at"`
	ETASeconds    int                   `json:"eta_seconds"`
}

type IntboxOnboardingResponse struct {
	Logins []*IntboxOnboardingLogin `json:"logins"`
}

func (adm *intboxAdmission) status(loginID networkid.UserLoginID) *IntboxOnboardingLogin {
	adm.lock.Lock()
	defer adm.lock.Unlock()
	resp := &IntboxOnboardingLogin{
		LoginID: loginID,
		State:   intboxImportIdle,
	}
	imp := adm.imports[loginID]
	if imp == nil {
		return resp
	}
	avgDuration := adm.avgDuration
	if imp.small {
		avgDuration = adm.avgSmallDuration
	}
	if avgDuration <= 0 {
		avgDuration = intboxDefaultImportDuration
	}
	resp.State = imp.state
	resp.RoomsTotal = imp.roomsTotal
	resp.RoomsDone = imp.roomsDone
	resp.MessagesSent = imp.messagesSent
	if !imp.startedAt.IsZero() {
		resp.StartedAt = imp.startedAt.UnixMilli()
	}
	now := time.Now()
	switch imp.state {
	case intboxImportQueued:
		resp.QueuePosition = adm.queuePositionLocked(imp)
		resp.ETASeconds = int(adm.queueWaitLocked(max(resp.QueuePosition, 1), imp.small, avgDuration, now).Seconds())
	case intboxImportImporting:
		resp.ETASeconds = int(importRemaining(imp, avgDuration, now).Seconds())
	}
	return resp
}

// importRemaining estimates how much longer an admitted import takes, from its progress so far
// (or the average import duration before its first room is done).
func importRemaining(imp *intboxImport, avgDuration time.Duration, now time.Time) time.Duration {
	if imp.roomsDone <= 0 {
		return avgDuration
	}
	remaining := max(imp.roomsTotal-imp.roomsDone, 0)
	perRoom := now.Sub(imp.startedAt) / time.Duration(imp.roomsDone)
	return perRoom * time.Duration(remaining)
}

// queueWaitLocked estimates how long the import at the given 1-based queue position waits before it's admitted
// (the app shows it as "starts in about N min"). There are max_concurrent_imports slots: each admitted import frees
// its slot when it's estimated to finish, and every import queued ahead takes a slot for about avgDuration.
// The early admissions of min_rooms_in_flight only make the real wait shorter. Must be called with the lock held.
//
// It used to be position x avgDuration, as if one login imported at a time: with 6 slots the 12th in line was told
// about 12 imports' time instead of about 2.
//
// The lanes are estimated separately (position is within the lane, small selects the small-import lane). The heavy
// limit isn't modelled: a heavy import may wait longer than estimated, and a normal one behind it less.
func (adm *intboxAdmission) queueWaitLocked(position int, small bool, avgDuration time.Duration, now time.Time) time.Duration {
	slots := 1
	if adm.cfg != nil && adm.cfg.Admission.Enabled {
		if small {
			slots = max(adm.cfg.Admission.SmallImports.MaxConcurrent, 1)
		} else {
			slots = max(adm.cfg.Admission.MaxConcurrentImports, 1)
		}
	}
	// When each slot holder finishes, relative to now. There can be more holders than slots: later history sync rounds
	// of logins that were already admitted don't queue.
	ends := make([]time.Duration, 0, slots+position)
	for _, imp := range adm.imports {
		if imp.holding && imp.small == small {
			ends = append(ends, importRemaining(imp, avgDuration, now))
		}
	}
	slices.Sort(ends)
	var start time.Duration
	for i := 0; i < position; i++ {
		// The next queued import starts once fewer than slots imports hold one.
		for len(ends) >= slots {
			start = max(start, ends[0])
			ends = ends[1:]
		}
		end := start + avgDuration
		idx, _ := slices.BinarySearch(ends, end)
		ends = slices.Insert(ends, idx, end)
	}
	return start
}

// getIntboxOnboarding implements GET /v3/intbox/onboarding (contract C-2.2).
func (wa *WhatsAppConnector) getIntboxOnboarding(w http.ResponseWriter, r *http.Request, user *bridgev2.User) {
	resp := &IntboxOnboardingResponse{Logins: make([]*IntboxOnboardingLogin, 0)}
	for _, loginID := range user.GetUserLoginIDs() {
		resp.Logins = append(resp.Logins, wa.intbox.admission.status(loginID))
	}
	exhttp.WriteJSONResponse(w, http.StatusOK, resp)
}
