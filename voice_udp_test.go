package discordgo

import (
	"context"
	"errors"
	"testing"
)

func TestVoiceUDPResolutionFailureReleasesConnectionLock(t *testing.T) {
	s, gateway := newVoiceTestSession(t)
	voice := newVoiceTestConnection(s)
	voice.op2 = voiceOP2{IP: "127.0.0.1", Port: 65536}
	err := voice.udpOpen(context.Background())
	if err == nil {
		t.Fatal("UDP resolution accepted an invalid port")
	}
	// failure needs the same lock to mark Dead and release voice waiters.
	// An early return from udpOpen must leave that teardown path usable.
	done := make(chan struct{})
	go func() {
		defer close(done)
		voice.failure(context.Background(), err)
	}()
	awaitVoiceTest(t, done, "UDP failure to finish connection teardown")
	if !errors.Is(voice.Err, err) || voice.Status != VoiceConnectionStatusDead {
		t.Fatalf("UDP failure left status %d and error %v", voice.Status, voice.Err)
	}
	select {
	case <-voice.DeadChannel():
	default:
		t.Fatal("UDP failure did not release Dead-channel waiters")
	}
	if leaves := gateway.leaves(t, s); leaves != 1 {
		t.Fatalf("UDP failure sent %d gateway leaves, want 1", leaves)
	}
	voice.Kill()
}
