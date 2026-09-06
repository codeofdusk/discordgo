package discordgo

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

func voiceConditionWaiters() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	count := 0
	for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(stack, "sync.(*Cond).Wait") &&
			strings.Contains(stack, "discordgo.(*VoiceConnection).") {
			count++
		}
	}
	return count
}

func TestVoiceSessionIDTimeoutStopsWaiters(t *testing.T) {
	s, _ := newWebsocketTestSession(t)
	voice := newWebsocketTestVoice(s)
	voice.Status = VoiceConnectionStatusNew
	before := voiceConditionWaiters()
	// A voice server update without its matching voice state update has no
	// session ID. Even a failed handshake must release all Cond waiters.
	voice.websocket(context.Background(), "unused.invalid", "unused")
	defer func() {
		voice.Cond.L.Lock()
		voice.sessionID = "test cleanup"
		voice.Cond.Broadcast()
		voice.Cond.L.Unlock()
	}()
	if voice.Status != VoiceConnectionStatusDead || len(s.VoiceConnections) != 0 {
		t.Fatal("timed-out connection was not killed and removed")
	}
	if !errors.Is(voice.Err, ErrVoiceNoSessionID) {
		t.Fatalf("voice error = %v, want ErrVoiceNoSessionID", voice.Err)
	}
	if after := voiceConditionWaiters(); after != before {
		t.Fatalf("voice condition waiters grew from %d to %d", before, after)
	}
}

func TestVoiceSessionIDWaitCancelsBeforeTimeout(t *testing.T) {
	s, _ := newWebsocketTestSession(t)
	voice := newWebsocketTestVoice(s)
	voice.Status = VoiceConnectionStatusNew
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		voice.websocket(ctx, "unused.invalid", "unused")
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cancelled handshake waited for the session-ID timeout")
	}
	if voice.Err != nil {
		t.Fatalf("cancelled handshake reported a new failure: %v", voice.Err)
	}
	voice.Kill()
}

func TestVoiceReadinessWaitsStopOnKill(t *testing.T) {
	for _, dave := range []bool{false, true} {
		name := "voice"
		if dave {
			name = "DAVE"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := newWebsocketTestSession(t)
			voice := newWebsocketTestVoice(s)
			voice.Status = VoiceConnectionStatusNew
			voice.dave = &DAVESession{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if dave {
					done <- voice.WaitForDAVEReady(ctx)
					return
				}
				done <- voice.waitUntilStatus(ctx, VoiceConnectionStatusReady)
			}()
			voice.Kill()
			select {
			case err := <-done:
				if !errors.Is(err, ErrVoiceConnectionDead) {
					t.Fatalf("readiness error = %v, want ErrVoiceConnectionDead", err)
				}
			case <-time.After(time.Second):
				t.Fatal("readiness waiter survived connection death")
			}
		})
	}
}

func TestVoiceStatusWaitCancellation(t *testing.T) {
	s, _ := newWebsocketTestSession(t)
	voice := newWebsocketTestVoice(s)
	voice.Status = VoiceConnectionStatusNew
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- voice.waitUntilStatus(ctx, VoiceConnectionStatusReady) }()
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("wait error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("status wait missed context cancellation")
		}
	}
}
