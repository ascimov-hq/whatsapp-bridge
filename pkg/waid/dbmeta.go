// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2024 Tulir Asokan
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

package waid

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/json"
	"time"

	"go.mau.fi/util/exerrors"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/util/random"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
)

type UserLoginMetadata struct {
	WADeviceID      uint16        `json:"wa_device_id"`
	PhoneLastSeen   jsontime.Unix `json:"phone_last_seen"`
	PhoneLastPinged jsontime.Unix `json:"phone_last_pinged"`
	Timezone        string        `json:"timezone"`
	PushKeys        *PushKeys     `json:"push_keys,omitempty"`
	APNSEncPubKey   []byte        `json:"apns_enc_pubkey,omitempty"`
	APNSEncPrivKey  []byte        `json:"apns_enc_privkey,omitempty"`
	LoggedInAt      jsontime.Unix `json:"logged_in_at,omitempty"`

	// intbox: start of the current history import of this WhatsApp account. Unlike LoggedInAt, it's kept
	// when the same account is paired again within intbox.reimport_after, so a re-pair doesn't import the
	// same chats again. Logins created before this field existed don't have it: use GetImportEpoch.
	ImportEpoch jsontime.Unix `json:"import_epoch,omitzero"`
	// intbox: when the last contact resync of this login finished (cooldown for automatic resyncs).
	LastContactResync jsontime.Unix `json:"last_contact_resync,omitzero"`

	AppStateRecoveryAttempted map[appstate.WAPatchName]time.Time `json:"app_state_recovery_attempted,omitempty"`

	HistorySyncPortalsNeedCreating bool `json:"history_sync_portals_need_creating,omitempty"`

	MData json.RawMessage `json:"mdata,omitempty"`
}

// GetImportEpoch returns the start of the current history import: ImportEpoch, or LoggedInAt for logins
// that were created before ImportEpoch existed (for those the import always started at the last pairing).
func (m *UserLoginMetadata) GetImportEpoch() jsontime.Unix {
	if m == nil {
		return jsontime.Unix{}
	} else if !m.ImportEpoch.IsZero() {
		return m.ImportEpoch
	}
	return m.LoggedInAt
}

// ImportEpochCarried returns true if the import epoch started before the current pairing, i.e. this pairing
// is a re-pair of an account whose import state was kept (see intbox.reimport_after).
func (m *UserLoginMetadata) ImportEpochCarried() bool {
	epoch := m.GetImportEpoch()
	return !epoch.IsZero() && !m.LoggedInAt.IsZero() && epoch.Before(m.LoggedInAt.Time)
}

type PushKeys struct {
	P256DH  []byte `json:"p256dh"`
	Auth    []byte `json:"auth"`
	Private []byte `json:"private"`
}

func (m *UserLoginMetadata) GeneratePushKeys() {
	privateKey := exerrors.Must(ecdh.P256().GenerateKey(rand.Reader))
	m.PushKeys = &PushKeys{
		P256DH:  privateKey.Public().(*ecdh.PublicKey).Bytes(),
		Auth:    random.Bytes(16),
		Private: privateKey.Bytes(),
	}
}

func (m *UserLoginMetadata) LoadTimezone() (*time.Location, error) {
	if m == nil || m.Timezone == "" {
		return nil, nil
	}
	return time.LoadLocation(m.Timezone)
}

type MessageErrorType string

const (
	MsgNoError             MessageErrorType = ""
	MsgErrDecryptionFailed MessageErrorType = "decryption_failed"
	MsgErrMediaNotFound    MessageErrorType = "media_not_found"
)

type GroupInviteMeta struct {
	JID        types.JID `json:"jid"`
	Code       string    `json:"code"`
	Expiration int64     `json:"expiration,string"`
	Inviter    types.JID `json:"inviter"`

	GroupName     string `json:"group_name,omitempty"`
	IsParentGroup bool   `json:"is_parent_group,omitempty"`
}

type MessageMetadata struct {
	SenderDeviceID   uint16            `json:"sender_device_id,omitempty"`
	Error            MessageErrorType  `json:"error,omitempty"`
	BroadcastListJID *types.JID        `json:"broadcast_list_jid,omitempty"`
	GroupInvite      *GroupInviteMeta  `json:"group_invite,omitempty"`
	FailedMediaMeta  json.RawMessage   `json:"media_meta,omitempty"`
	DirectMediaMeta  json.RawMessage   `json:"direct_media_meta,omitempty"`
	IsMatrixPoll     bool              `json:"is_matrix_poll,omitempty"`
	Edits            []types.MessageID `json:"edits,omitempty"`
}

func (mm *MessageMetadata) CopyFrom(other any) {
	otherMM := other.(*MessageMetadata)
	mm.SenderDeviceID = otherMM.SenderDeviceID
	mm.Error = otherMM.Error
	if otherMM.BroadcastListJID != nil {
		mm.BroadcastListJID = otherMM.BroadcastListJID
	}
	if otherMM.FailedMediaMeta != nil {
		mm.FailedMediaMeta = otherMM.FailedMediaMeta
	}
	if otherMM.DirectMediaMeta != nil {
		mm.DirectMediaMeta = otherMM.DirectMediaMeta
	}
	if otherMM.GroupInvite != nil {
		mm.GroupInvite = otherMM.GroupInvite
	}
	mm.IsMatrixPoll = mm.IsMatrixPoll || otherMM.IsMatrixPoll
}

type ReactionMetadata struct {
	SenderDeviceID uint16 `json:"sender_device_id,omitempty"`
}

type PortalMetadata struct {
	DisappearingTimerSetAt     int64                `json:"disappearing_timer_set_at,omitempty"`
	TopicID                    string               `json:"topic_id,omitempty"`
	LastSync                   jsontime.Unix        `json:"last_sync,omitempty"`
	CommunityAnnouncementGroup bool                 `json:"is_cag,omitempty"`
	AddressingMode             types.AddressingMode `json:"addressing_mode,omitempty"`
	LIDMigrationAttempted      bool                 `json:"lid_migration_attempted,omitempty"`
}

type GhostMetadata struct {
	LastSync jsontime.Unix `json:"last_sync,omitempty"`
}
