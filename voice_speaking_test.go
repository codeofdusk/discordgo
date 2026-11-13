package discordgo

import (
	"testing"
	"time"
)

// A voice server update starts a new voice session on the same connection,
// as happens when the session user is moved to another channel. The speaking
// announcement made on the old session does not carry over to the new one.
func TestVoiceServerUpdateForgetsSpeakingAnnouncement(t *testing.T) {
	s, _ := newVoiceTestSession(t)
	endpoint := newVoiceTestEndpoint(t, s)
	endpoint.proceed()
	voice := newVoiceTestConnection(s)
	voice.speaking = true

	update := &VoiceServerUpdate{GuildID: "guild", Token: "token", Endpoint: &endpoint.address}
	if err := voice.onVoiceServerUpdate(update); err != nil {
		t.Fatal(err)
	}
	select {
	case op := <-endpoint.identified:
		if op != 0 {
			t.Fatalf("new voice session opened with op %d, want identify", op)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("new voice session never identified")
	}

	voice.Cond.L.Lock()
	speaking := voice.speaking
	voice.Cond.L.Unlock()
	if speaking {
		t.Fatal("new voice session inherited the previous session's speaking announcement")
	}
	voice.Kill()
}
