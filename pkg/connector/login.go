package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exsync"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

const (
	LoginStepIDQR          = "fi.mau.whatsapp.login.qr"
	LoginStepIDPhoneNumber = "fi.mau.whatsapp.login.phone"
	LoginStepIDCode        = "fi.mau.whatsapp.login.code"
	LoginStepIDComplete    = "fi.mau.whatsapp.login.complete"

	LoginFlowIDQR    = "qr"
	LoginFlowIDPhone = "phone"
)

func (wa *WhatsAppConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{
		Name:        "QR",
		Description: "Scan a QR code to pair the bridge to your WhatsApp account",
		ID:          LoginFlowIDQR,
	}, {
		Name:        "Pairing code",
		Description: "Input your phone number to get a pairing code, to pair the bridge to your WhatsApp account",
		ID:          LoginFlowIDPhone,
	}}
}

var (
	ErrLoginClientOutdated = bridgev2.RespError{
		ErrCode:    "FI.MAU.WHATSAPP.CLIENT_OUTDATED",
		Err:        "Got client outdated error while waiting for QRs. The bridge must be updated to continue.",
		StatusCode: http.StatusInternalServerError,
	}
	ErrLoginMultideviceNotEnabled = bridgev2.RespError{
		ErrCode:    "FI.MAU.WHATSAPP.MULTIDEVICE_NOT_ENABLED",
		Err:        "Please enable WhatsApp web multidevice and scan the QR code again.",
		StatusCode: http.StatusBadRequest,
	}
	ErrLoginTimeout = bridgev2.RespError{
		ErrCode:    "FI.MAU.WHATSAPP.LOGIN_TIMEOUT",
		Err:        "Entering code or scanning QR timed out. Please try again.",
		StatusCode: http.StatusBadRequest,
	}
	ErrUnexpectedDisconnect = bridgev2.RespError{
		ErrCode:    "FI.MAU.WHATSAPP.LOGIN_UNEXPECTED_EVENT",
		Err:        "Unexpected event while waiting for login",
		StatusCode: http.StatusInternalServerError,
	}
	ErrPhoneNumberTooShort = bridgev2.RespError{
		ErrCode:    "FI.MAU.WHATSAPP.PHONE_NUMBER_TOO_SHORT",
		Err:        "Phone number too short",
		StatusCode: http.StatusBadRequest,
	}
	ErrPhoneNumberIsNotInternational = bridgev2.RespError{
		ErrCode:    "FI.MAU.WHATSAPP.PHONE_NUMBER_NOT_INTERNATIONAL",
		Err:        "Phone number must be in international format",
		StatusCode: http.StatusBadRequest,
	}
	ErrRateLimitedByWhatsApp = bridgev2.RespError{
		ErrCode:    "FI.MAU.WHATSAPP.RATE_LIMITED",
		Err:        "Rate limited by WhatsApp. Try again later.",
		StatusCode: http.StatusBadRequest,
	}
)

func (wa *WhatsAppConnector) CreateLogin(_ context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	return &WALogin{
		User:      user,
		Main:      wa,
		PhoneCode: flowID == LoginFlowIDPhone,
		Log: user.Log.With().
			Str("action", "login").
			Bool("phone_code", flowID == LoginFlowIDPhone).
			Logger(),

		WaitForQRs:    exsync.NewEvent(),
		LoginComplete: exsync.NewEvent(),
		Received515:   exsync.NewEvent(),

		finished: make(chan struct{}),
	}, nil
}

type WALogin struct {
	User      *bridgev2.User
	Main      *WhatsAppConnector
	Client    *whatsmeow.Client
	Log       zerolog.Logger
	PhoneCode bool
	Timezone  string

	QRs           []string
	StartTime     time.Time
	LoginError    error
	LoginSuccess  *events.PairSuccess
	WaitForQRs    *exsync.Event
	LoginComplete *exsync.Event
	Received515   *exsync.Event
	PrevQRIndex   atomic.Int32

	Closed         atomic.Bool
	EventHandlerID uint32

	// intbox (round 2): one pending pairing code per phone number, completing state, explicit re-login.
	// stateLock protects LoginError, LoginSuccess, superseded and phone.
	stateLock  sync.Mutex
	superseded bool
	// The normalized phone number this process claimed in WhatsAppConnector.pendingPhoneLogins
	phone string
	// Set after PairSuccess, until the login process finishes (see IsCompleting)
	completing atomic.Bool
	// The login this process was started to re-login (?login_id= in the provisioning API)
	overrideLoginID networkid.UserLoginID
	// Closed when the process finishes (completed, failed or cancelled)
	finished   chan struct{}
	finishOnce sync.Once
}

var (
	_ bridgev2.LoginProcessDisplayAndWait = (*WALogin)(nil)
	_ bridgev2.LoginProcessUserInput      = (*WALogin)(nil)
	_ bridgev2.LoginProcessWithOverride   = (*WALogin)(nil)
	_ bridgev2.LoginProcessCompleting     = (*WALogin)(nil)
	_ bridgev2.LoginProcessSuperseded     = (*WALogin)(nil)
)

// IsCompleting implements bridgev2.LoginProcessCompleting: it returns true after WhatsApp accepted the pairing
// (PairSuccess), while the process waits for the reconnect that follows pairing and creates the login.
// Cancelling the process at that point would leave a linked device on the phone that the bridge never uses,
// so the provisioning API never supersedes it, and neither does a newer pairing code request for the same number.
func (wl *WALogin) IsCompleting() bool {
	return wl.completing.Load()
}

// IsSuperseded implements bridgev2.LoginProcessSuperseded: it returns true once a newer pairing code for the same
// phone number cancelled this process (see claimPhone). An explicit re-login submits the phone number inside
// StartWithOverride, so this can happen before the provisioning API registered the process, and the newer code
// can belong to the older of two overlapping start requests. The provisioning API then keeps that one.
func (wl *WALogin) IsSuperseded() bool {
	return wl.isSuperseded()
}

// closeClient stops the pre-pairing whatsmeow client of this process. It's safe to call more than once.
func (wl *WALogin) closeClient() {
	wl.Closed.Store(true)
	if wl.Client != nil {
		wl.Client.RemoveEventHandler(wl.EventHandlerID)
		wl.Client.Disconnect()
	}
}

// finish marks the process as finished: it's no longer completing, its phone number claim is released and
// processes waiting for it are woken up.
func (wl *WALogin) finish() {
	wl.finishOnce.Do(func() {
		wl.completing.Store(false)
		wl.releasePhone()
		close(wl.finished)
	})
}

func (wl *WALogin) setLoginError(err error) {
	wl.stateLock.Lock()
	defer wl.stateLock.Unlock()
	if wl.superseded {
		return
	}
	wl.LoginError = err
}

func (wl *WALogin) getResult() (*events.PairSuccess, error) {
	wl.stateLock.Lock()
	defer wl.stateLock.Unlock()
	return wl.LoginSuccess, wl.LoginError
}

func (wl *WALogin) isSuperseded() bool {
	wl.stateLock.Lock()
	defer wl.stateLock.Unlock()
	return wl.superseded
}

// supersede cancels this process because a newer pairing code was requested for the same phone number.
// Its pending Wait returns bridgev2.ErrLoginSuperseded (HTTP 410). It returns false and does nothing if the
// process already paired (it's completing) and can't be cancelled anymore.
func (wl *WALogin) supersede() bool {
	wl.stateLock.Lock()
	if wl.completing.Load() || wl.LoginSuccess != nil {
		wl.stateLock.Unlock()
		return false
	}
	wl.superseded = true
	wl.LoginError = bridgev2.ErrLoginSuperseded
	wl.stateLock.Unlock()
	wl.Log.Info().Msg("Cancelling login process, a newer pairing code was requested for the same phone number")
	wl.closeClient()
	wl.LoginComplete.Set()
	return true
}

func (wl *WALogin) releasePhone() {
	wl.stateLock.Lock()
	phone := wl.phone
	wl.phone = ""
	wl.stateLock.Unlock()
	wl.Main.pendingPhoneLogins.release(phone, wl)
}

// waitForCompletingLogin waits (up to completingLoginWait) if an older login process of the phone number
// already paired and is only finishing up, then refuses with ALREADY_LOGGED_IN if it left a connected login.
func (wl *WALogin) waitForCompletingLogin(ctx context.Context, phone string) error {
	prev := wl.Main.pendingPhoneLogins.get(phone)
	if prev == nil || prev == wl || !prev.IsCompleting() {
		return nil
	}
	wl.Log.Info().Msg("An older login process for the same phone number already paired, waiting for it to finish")
	select {
	case <-prev.finished:
	case <-time.After(completingLoginWait):
		wl.Log.Warn().Msg("Older login process for the same phone number didn't finish in time, continuing")
	case <-ctx.Done():
		return ctx.Err()
	}
	return wl.checkExistingLogin(phone)
}

// claimPhone makes this process the one with the pending pairing code for the phone number, and supersedes
// the older process of the same number, unless that one already paired. It's called after WhatsApp issued
// the new code, so a failed code request (e.g. rate limited) leaves the older code usable.
func (wl *WALogin) claimPhone(phone string) {
	prev := wl.Main.pendingPhoneLogins.claim(phone, wl)
	wl.stateLock.Lock()
	wl.phone = phone
	wl.stateLock.Unlock()
	if prev != nil && !prev.supersede() {
		wl.Log.Warn().Msg("Older login process for the same phone number paired while a new code was requested")
	}
}

// checkExistingLogin returns bridgev2.ErrAlreadyLoggedIn (HTTP 409) if the same Matrix user already has a
// login for the phone number whose client is connected right now, unless this process was started as an
// explicit re-login of that login (?login_id=). Pairing again would only link a duplicate device.
func (wl *WALogin) checkExistingLogin(phone string) error {
	if !wl.Main.Config.Intbox.RefuseConnectedRepair {
		return nil
	}
	loginID := networkid.UserLoginID(phone)
	ul := wl.Main.Bridge.GetCachedUserLoginByID(loginID)
	if ul == nil {
		return nil
	}
	var clientCurrent, clientConnected bool
	if wac, ok := ul.Client.(*WhatsAppClient); ok && wac != nil {
		clientCurrent = wac.isCurrent()
		clientConnected = wac.IsLoggedIn() && wac.Client.IsConnected()
	}
	state := ul.BridgeState.GetPrev().StateEvent
	if !shouldRefuseRepair(ul.UserMXID == wl.User.MXID, wl.overrideLoginID == loginID, clientCurrent, clientConnected, state) {
		return nil
	}
	wl.Log.Info().Msg("Refusing to pair a phone number that already has a connected login")
	resp := bridgev2.ErrAlreadyLoggedIn
	resp.ExtraData = map[string]any{"login_id": string(ul.ID)}
	return resp
}

const LoginConnectWait = 15 * time.Second

func (wl *WALogin) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	wl.Main.firstClientConnectOnce.Do(wl.Main.onFirstClientConnect)
	device := wl.Main.DeviceStore.NewDevice()
	wl.Client = whatsmeow.NewClient(device, waLog.Zerolog(wl.Log))
	wl.Client.EnableAutoReconnect = false
	wl.Client.DisableLoginAutoReconnect = true
	wl.EventHandlerID = wl.Client.AddEventHandler(wl.handleEvent)
	if err := wl.Main.updateProxy(ctx, wl.Client, true); err != nil {
		return nil, err
	}

	if wl.PhoneCode {
		return &bridgev2.LoginStep{
			Type:   bridgev2.LoginStepTypeUserInput,
			StepID: LoginStepIDPhoneNumber,
			UserInputParams: &bridgev2.LoginUserInputParams{
				Fields: []bridgev2.LoginInputDataField{{
					Type:        bridgev2.LoginInputFieldTypePhoneNumber,
					ID:          "phone_number",
					Name:        "Phone number",
					Description: "Your WhatsApp phone number in international format",
				}},
			},
		}, nil
	}
	err := wl.Client.Connect()
	if err != nil {
		wl.Log.Err(err).Msg("Failed to connect to WhatsApp for QR login")
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, LoginConnectWait)
	defer cancel()
	err = wl.WaitForQRs.Wait(ctx)
	if err != nil {
		wl.Log.Warn().Err(err).Msg("Timed out waiting for first QR")
		wl.closeClient()
		return nil, err
	}
	return makeQRStep(wl.QRs[0]), nil
}

func (wl *WALogin) StartWithOverride(ctx context.Context, old *bridgev2.UserLogin) (*bridgev2.LoginStep, error) {
	if old != nil {
		// intbox: an explicit re-login of this login may pair its phone number even if the login is connected
		wl.overrideLoginID = old.ID
	}
	step, err := wl.Start(ctx)
	if err == nil && step != nil && old != nil && step.StepID == LoginStepIDPhoneNumber {
		phoneNumber := fmt.Sprintf("+%s", old.ID)
		wl.Log.Debug().
			Str("phone_number", phoneNumber).
			Msg("Auto-submitting phone number for relogin")
		return wl.SubmitUserInput(ctx, map[string]string{
			"phone_number": phoneNumber,
		})
	}
	return step, err
}

func (wl *WALogin) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	// intbox: refuse to pair a number whose login is connected (409), and keep one pending code per number:
	// a newer code supersedes the older process (410 for it). Numbers whatsmeow rejects are left to it.
	phone := normalizePairingPhone(input["phone_number"])
	trackPhone := wl.Main.Config.Intbox.OneCodePerPhone && len(phone) > 6 && !strings.HasPrefix(phone, "0")
	if len(phone) > 6 {
		if err := wl.checkExistingLogin(phone); err != nil {
			return nil, err
		}
	}
	if trackPhone {
		if err := wl.waitForCompletingLogin(ctx, phone); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, LoginConnectWait)
	defer cancel()
	err := wl.Client.Connect()
	if err != nil {
		wl.Log.Err(err).Msg("Failed to connect to WhatsApp for phone code login")
		return nil, err
	}
	err = wl.WaitForQRs.Wait(ctx)
	if err != nil {
		wl.Log.Warn().Err(err).Msg("Timed out waiting for connection")
		return nil, fmt.Errorf("failed to wait for connection: %w", err)
	}
	pairingCode, err := wl.Client.PairPhone(ctx, input["phone_number"], true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		wl.Log.Err(err).Msg("Failed to request phone code login")
		if errors.Is(err, whatsmeow.ErrPhoneNumberTooShort) {
			return nil, ErrPhoneNumberTooShort
		} else if errors.Is(err, whatsmeow.ErrPhoneNumberIsNotInternational) {
			return nil, ErrPhoneNumberIsNotInternational
		} else if errors.Is(err, whatsmeow.ErrIQRateOverLimit) {
			return nil, ErrRateLimitedByWhatsApp
		}
		return nil, err
	}
	if trackPhone {
		wl.claimPhone(phone)
	}
	wl.Log.Debug().Msg("Phone code login started")
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeDisplayAndWait,
		StepID:       LoginStepIDCode,
		Instructions: "Input the pairing code in the WhatsApp mobile app to log in",
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
			Type: bridgev2.LoginDisplayTypeCode,
			Data: pairingCode,
		},
	}, nil
}

func makeQRStep(qr string) *bridgev2.LoginStep {
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeDisplayAndWait,
		StepID:       LoginStepIDQR,
		Instructions: "Scan the QR code with the WhatsApp mobile app to log in",
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
			Type: bridgev2.LoginDisplayTypeQR,
			Data: qr,
		},
	}
}

var qrIntervals = []time.Duration{1 * time.Minute, 20 * time.Second, 20 * time.Second, 20 * time.Second, 20 * time.Second, 20 * time.Second}

func (wl *WALogin) getQRIndex() (currentIndex int, timeUntilNext time.Duration) {
	timeSinceStart := time.Since(wl.StartTime)
	var sum time.Duration
	for i, interval := range qrIntervals {
		if timeSinceStart > sum+interval {
			sum += interval
		} else {
			return i, interval - (timeSinceStart - sum)
		}
	}
	return -1, 0
}

func (wl *WALogin) handleEvent(rawEvt any) {
	if wl.Closed.Load() {
		return
	}
	switch evt := rawEvt.(type) {
	case *events.QR:
		wl.Log.Debug().Int("code_count", len(evt.Codes)).Msg("Received QR codes")
		wl.QRs = evt.Codes
		wl.StartTime = time.Now()
		wl.WaitForQRs.Set()
		return
	case *events.QRScannedWithoutMultidevice:
		wl.Log.Error().Msg("QR code scanned without multidevice enabled")
		wl.setLoginError(ErrLoginMultideviceNotEnabled)
	case *events.ClientOutdated:
		wl.Log.Error().Msg("Got client outdated error")
		wl.setLoginError(ErrLoginClientOutdated)
	case *events.PairSuccess:
		wl.Log.Info().Any("event_data", evt).Msg("Got pair successful event")
		wl.stateLock.Lock()
		if wl.superseded {
			wl.stateLock.Unlock()
			wl.Log.Warn().Msg("Got pair successful event after the login process was superseded")
			return
		}
		wl.LoginSuccess = evt
		// intbox: from now on the process must not be cancelled to make room for a newer one
		wl.completing.Store(true)
		wl.stateLock.Unlock()
	case *events.PairError:
		wl.Log.Error().Any("event_data", evt).Msg("Got pair error event")
		wl.setLoginError(bridgev2.RespError{
			ErrCode:    "FI.MAU.WHATSAPP.PAIR_ERROR",
			Err:        evt.Error.Error(),
			StatusCode: http.StatusInternalServerError,
		})
	case *events.Disconnected:
		wl.Log.Warn().Msg("Got disconnected event (login timed out)")
		wl.setLoginError(ErrLoginTimeout)
	case *events.Connected, *events.ConnectFailure, *events.LoggedOut, *events.TemporaryBan:
		wl.Log.Warn().Any("event_data", evt).Type("event_type", evt).Msg("Got unexpected disconnect event")
		wl.setLoginError(ErrUnexpectedDisconnect)
	case *events.ManualLoginReconnect:
		wl.Received515.Set()
	default:
		wl.Log.Warn().Type("event_type", evt).Msg("Got unexpected event")
		return
	}
	wl.LoginComplete.Set()
}

func (wl *WALogin) Wait(ctx context.Context) (step *bridgev2.LoginStep, retErr error) {
	defer func() {
		// intbox: the process is finished when it fails or completes (a QR step only returns the next code)
		if retErr != nil || (step != nil && step.Type == bridgev2.LoginStepTypeComplete) {
			wl.finish()
		}
	}()
	if wl.PhoneCode {
		err := wl.LoginComplete.Wait(ctx)
		if err != nil && !wl.IsCompleting() {
			wl.closeClient()
			return nil, err
		}
	} else {
		prevIndex := int(wl.PrevQRIndex.Load())
		currentIndex, timeUntilNext := wl.getQRIndex()
		nextIndex := currentIndex + 1
		logEvt := wl.Log.Debug().
			Int("prev_index", prevIndex).
			Int("current_index", currentIndex)
		if currentIndex > prevIndex {
			logEvt.Msg("Returning new QR immediately")
			wl.PrevQRIndex.Store(int32(currentIndex))
			return makeQRStep(wl.QRs[currentIndex]), nil
		}
		logEvt.Int("next_index", nextIndex).Stringer("time_until_next", timeUntilNext).Msg("Waiting for next QR")
		select {
		case <-time.After(timeUntilNext):
			if nextIndex < 0 || nextIndex >= len(wl.QRs) {
				wl.Log.Debug().Msg("No more QRs to return")
				wl.closeClient()
				return nil, ErrLoginTimeout
			}
			wl.PrevQRIndex.Store(int32(nextIndex))
			return makeQRStep(wl.QRs[nextIndex]), nil
		case <-ctx.Done():
			if !wl.IsCompleting() {
				wl.closeClient()
				return nil, ctx.Err()
			}
		case <-wl.LoginComplete.GetChan():
		}
	}
	loginSuccess, loginErr := wl.getResult()
	if loginErr != nil {
		wl.Log.Debug().Err(loginErr).Msg("Login completed with error")
		wl.closeClient()
		return nil, loginErr
	} else if loginSuccess == nil {
		// Only the 515 reconnect arrived, which never happens before PairSuccess
		wl.Log.Warn().Msg("Login completed without pair success event")
		wl.closeClient()
		return nil, ErrUnexpectedDisconnect
	}
	wl.Log.Debug().Msg("Login completed without error, waiting for 515 event")
	// intbox: WhatsApp accepted the pairing, so finish creating the login even if the step's context is cancelled
	// now (the process was superseded or cancelled just before it reported IsCompleting). Otherwise the phone
	// would keep a linked device that the bridge never uses.
	completeCtx := context.WithoutCancel(ctx)
	ctxTimeout, cancel := context.WithTimeout(completeCtx, 10*time.Second)
	err := wl.Received515.Wait(ctxTimeout)
	cancel()
	wl.closeClient()
	if err != nil {
		return nil, fmt.Errorf("timed out waiting for 515: %w", err)
	}
	return wl.completeLogin(completeCtx, loginSuccess)
}

// completeLogin creates (or reuses) the UserLogin for a successful pairing and connects its client.
func (wl *WALogin) completeLogin(ctx context.Context, pairSuccess *events.PairSuccess) (*bridgev2.LoginStep, error) {
	newLoginID := waid.MakeUserLoginID(pairSuccess.ID)
	now := jsontime.UnixNow()
	meta := &waid.UserLoginMetadata{
		WADeviceID: pairSuccess.ID.Device,
		LoggedInAt: now,
		// intbox: a new pairing starts a new history import, unless the import state is carried below
		ImportEpoch: now,
		Timezone:    wl.Timezone,

		HistorySyncPortalsNeedCreating: true,
	}
	existing, err := wl.Main.Bridge.GetExistingUserLoginByID(ctx, newLoginID)
	if err != nil {
		wl.Log.Warn().Err(err).Msg("Failed to get existing login for the paired account")
	} else if existing != nil {
		if existing.UserMXID == wl.User.MXID {
			prevMeta, _ := existing.Metadata.(*waid.UserLoginMetadata)
			if carryImportState(prevMeta, meta, wl.Main.Config.Intbox.ReimportAfter, now.Time) {
				wl.Log.Info().
					Time("import_epoch", meta.ImportEpoch.Time).
					Msg("Account was paired again within intbox.reimport_after, keeping its history import state")
			}
		}
		// The account was paired again with a new companion device: unlink the previous one, so that duplicate
		// "Chrome (Linux)" devices don't pile up on the phone and the old socket can't touch the login.
		wl.retireReplacedDevice(ctx, existing, pairSuccess.ID)
	}
	ul, err := wl.User.NewLogin(ctx, &database.UserLogin{
		ID:         newLoginID,
		RemoteName: "+" + pairSuccess.ID.User,
		RemoteProfile: status.RemoteProfile{
			Phone: "+" + pairSuccess.ID.User,
			Name:  pairSuccess.BusinessName,
		},
		Metadata: meta,
	}, &bridgev2.NewLoginParams{
		DeleteOnConflict: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create user login: %w", err)
	}

	ul.Client.(*WhatsAppClient).isNewLogin = true
	ul.Client.Connect(ul.Log.WithContext(wl.Main.Bridge.BackgroundCtx))

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       LoginStepIDComplete,
		Instructions: fmt.Sprintf("Successfully logged in as %s", ul.RemoteName),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: ul.ID,
			UserLogin:   ul,
		},
	}, nil
}

// retireReplacedDevice retires the client of an existing login of the paired account if it uses another
// companion device than the new pairing, logging that device out remotely (best effort: it may already be
// unlinked). It waits a few seconds for the remote logout, so that it's done before the new client connects.
func (wl *WALogin) retireReplacedDevice(ctx context.Context, existing *bridgev2.UserLogin, newDevice types.JID) {
	old, ok := existing.Client.(*WhatsAppClient)
	if !ok || old == nil || old.JID.IsEmpty() || (old.JID.User == newDevice.User && old.JID.Device == newDevice.Device) {
		// No previous device, or the same device (LoadUserLogin retires the instance without a logout)
		return
	}
	log := wl.Log.With().
		Uint16("old_device_id", old.JID.Device).
		Uint16("new_device_id", newDevice.Device).
		Logger()
	log.Info().Msg("Logging out the previous companion device of the paired account")
	select {
	case <-old.retire(log.WithContext(ctx), errClientReplacedByPair, true):
	case <-time.After(replacedDeviceLogoutWait):
		log.Warn().Msg("Logging out the previous companion device is taking long, continuing")
	}
}

func (wl *WALogin) Cancel() {
	if wl.completing.Load() {
		// intbox: WhatsApp already accepted the pairing. Closing the socket now would lose the reconnect that
		// follows pairing and leave a linked device the bridge never uses, so the process finishes in Wait.
		wl.Log.Info().Msg("Not cancelling login process that already paired, it's finishing up")
		return
	}
	wl.closeClient()
	// intbox: free the phone number for newer processes
	wl.releasePhone()
	wl.finish()
}
