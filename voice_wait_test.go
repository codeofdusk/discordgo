package discordgo

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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

// Pause the transition after it snapshots dave, then observe its next lock
// attempt. This places activation between a readiness check and Cond.Wait.
type daveTransitionTestLocker struct {
	mu              sync.Mutex
	locks           atomic.Int32
	firstUnlock     atomic.Bool
	snapshot        chan struct{}
	allowTransition chan struct{}
	notification    chan struct{}
}

func (l *daveTransitionTestLocker) Lock() {
	if l.locks.Add(1) == 3 {
		close(l.notification)
	}
	l.mu.Lock()
}

func (l *daveTransitionTestLocker) Unlock() {
	l.mu.Unlock()
	if l.firstUnlock.CompareAndSwap(false, true) {
		close(l.snapshot)
		<-l.allowTransition
	}
}

func TestDAVETransitionCannotMissReadinessWait(t *testing.T) {
	s := &Session{LogLevel: -1, VoiceConnections: make(map[string]*VoiceConnection)}
	voice := newWebsocketTestVoice(s)
	voice.dave = &DAVESession{
		senderKey:           []byte{1, 2, 3},
		frameCipher:         testAEAD{},
		pendingTransitionID: 1,
	}
	voice.pendingReWelcome = true
	locker := &daveTransitionTestLocker{
		snapshot:        make(chan struct{}),
		allowTransition: make(chan struct{}),
		notification:    make(chan struct{}),
	}
	voice.Cond = sync.NewCond(locker)
	var transitionOnce, waiterOnce sync.Once
	allowWait := make(chan struct{})
	releaseTransition := func() { transitionOnce.Do(func() { close(locker.allowTransition) }) }
	releaseWaiter := func() { waiterOnce.Do(func() { close(allowWait) }) }
	t.Cleanup(releaseTransition)
	t.Cleanup(releaseWaiter)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	transitionDone := make(chan struct{})
	go func() {
		defer close(transitionDone)
		voice.handleDAVEExecuteTransition([]byte(`{"transition_id":2}`))
	}()
	select {
	case <-locker.snapshot:
	case <-ctx.Done():
		t.Fatal("transition did not snapshot its DAVE session")
	}
	checked := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- voice.waitFor(ctx, func() bool {
			ready := voice.dave.CanEncrypt()
			if !ready {
				close(checked)
				<-allowWait
			}
			return ready
		})
	}()
	select {
	case <-checked:
	case <-ctx.Done():
		t.Fatal("waiter did not check readiness before activation")
	}
	releaseTransition()
	select {
	case <-locker.notification:
	case <-ctx.Done():
		t.Fatal("transition did not attempt to notify its readiness waiter")
	}
	releaseWaiter()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter missed successful DAVE activation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("waiter missed successful DAVE activation")
	}
	select {
	case <-transitionDone:
	case <-ctx.Done():
		t.Fatal("transition did not finish after notifying the waiter")
	}
}
