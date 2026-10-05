package waid

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.mau.fi/util/jsontime"
)

// TestUserLoginMetadataUpgrade makes sure that metadata stored by older bridge versions (no import epoch)
// keeps its behaviour: the import epoch is the last pairing, and nothing counts as carried.
func TestUserLoginMetadataUpgrade(t *testing.T) {
	var meta UserLoginMetadata
	stored := `{"wa_device_id":3,"phone_last_seen":0,"phone_last_pinged":0,"timezone":"","logged_in_at":1759550000,"history_sync_portals_need_creating":true}`
	if err := json.Unmarshal([]byte(stored), &meta); err != nil {
		t.Fatalf("failed to parse stored metadata: %v", err)
	}
	if !meta.ImportEpoch.IsZero() || !meta.LastContactResync.IsZero() {
		t.Fatal("old metadata has new fields set")
	}
	if epoch := meta.GetImportEpoch(); !epoch.Equal(meta.LoggedInAt.Time) {
		t.Fatalf("import epoch of old metadata should be LoggedInAt, got %v", epoch)
	}
	if meta.ImportEpochCarried() {
		t.Fatal("old metadata shouldn't count as a carried import epoch")
	}
	// The new fields aren't written while they're unset
	out, err := json.Marshal(&meta)
	if err != nil {
		t.Fatalf("failed to marshal metadata: %v", err)
	} else if strings.Contains(string(out), "import_epoch") || strings.Contains(string(out), "last_contact_resync") {
		t.Fatalf("unset fields were serialized: %s", out)
	}
	// Round trip with the new fields
	meta.ImportEpoch = jsontime.UInt(1759540000)
	meta.LastContactResync = jsontime.U(time.Unix(1759545000, 0))
	out, err = json.Marshal(&meta)
	if err != nil {
		t.Fatalf("failed to marshal metadata: %v", err)
	}
	var parsed UserLoginMetadata
	if err = json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("failed to parse metadata: %v", err)
	}
	if parsed.ImportEpoch.Unix() != 1759540000 || parsed.LastContactResync.Unix() != 1759545000 {
		t.Fatalf("round trip lost the new fields: %s", out)
	} else if !parsed.ImportEpochCarried() {
		t.Fatal("epoch before LoggedInAt should count as carried")
	}
	var nilMeta *UserLoginMetadata
	if !nilMeta.GetImportEpoch().IsZero() {
		t.Fatal("nil metadata should have a zero import epoch")
	}
}
