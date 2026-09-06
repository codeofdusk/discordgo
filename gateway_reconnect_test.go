package discordgo

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type gatewayTestTransport func(*http.Request) (*http.Response, error)

func (f gatewayTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type gatewayReconnectFixture struct {
	session    *Session
	connection *websocket.Conn
	generation context.Context
	gateway    string
	attempts   atomic.Int32
	done       chan struct{}
}

func startFailingGatewayReconnect(t *testing.T) *gatewayReconnectFixture {
	t.Helper()
	s := newGatewayTestSession(t, func(connection *websocket.Conn) {
		if err := sendGatewayHello(connection); err != nil {
			return
		}
		if _, _, err := connection.ReadMessage(); err != nil {
			return
		}
		if err := sendGatewayReady(connection); err != nil {
			return
		}
		for {
			if _, _, err := connection.ReadMessage(); err != nil {
				return
			}
		}
	})
	fixture := &gatewayReconnectFixture{session: s, gateway: s.gateway, done: make(chan struct{})}
	failed := make(chan struct{})
	dialer := *s.Dialer
	dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if fixture.attempts.Add(1) == 2 {
			close(failed)
			return nil, errors.New("test reconnect failure")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	s.Dialer = &dialer
	s.ShouldReconnectOnError = true
	// Unexpected gateway discovery must remain offline even if the regression
	// lets a cancelled retry escape after clearing its cached URL.
	s.Client.Transport = gatewayTestTransport(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected gateway discovery")
	})
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	s.RLock()
	fixture.connection = s.wsConn
	fixture.generation = s.gatewayContext
	s.RUnlock()
	go func() {
		defer close(fixture.done)
		s.reconnectConnection(fixture.connection, websocket.CloseServiceRestart, false)
	}()
	select {
	case <-failed:
	case <-time.After(3 * time.Second):
		t.Fatal("reconnect did not reach the injected dial failure")
	}
	// Wait until the attempt has released the session lock. The next retry
	// cannot run for a second, leaving a deterministic cancellation window.
	s.Lock()
	s.Unlock()
	return fixture
}

func TestCloseCancelsSleepingGatewayReconnect(t *testing.T) {
	fixture := startFailingGatewayReconnect(t)
	if err := fixture.session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fixture.done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close left the reconnect loop asleep")
	}
	if count := fixture.attempts.Load(); count != 2 {
		t.Fatalf("dial attempts = %d, want 2", count)
	}
}

func TestAutomaticGatewayReconnectRetainsGeneration(t *testing.T) {
	fixture := startFailingGatewayReconnect(t)
	s := fixture.session
	s.Lock()
	s.gateway = fixture.gateway
	s.Unlock()
	select {
	case <-fixture.done:
	case <-time.After(3 * time.Second):
		t.Fatal("automatic reconnect did not establish a replacement")
	}
	s.RLock()
	connected := s.wsConn != nil && s.wsConn != fixture.connection
	sameGeneration := s.gatewayContext == fixture.generation && s.gatewayContext.Err() == nil
	s.RUnlock()
	if !connected || !sameGeneration {
		t.Fatal("automatic reconnect did not preserve the active gateway generation")
	}
	if count := fixture.attempts.Load(); count != 3 {
		t.Fatalf("dial attempts = %d, want 3", count)
	}
}

func TestOldGatewayGenerationCannotAffectExplicitReopen(t *testing.T) {
	fixture := startFailingGatewayReconnect(t)
	s := fixture.session
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s.Lock()
	s.gateway = fixture.gateway
	s.Unlock()
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	s.RLock()
	connection := s.wsConn
	generation := s.gatewayContext
	sessionID := s.sessionID
	s.RUnlock()
	if generation == fixture.generation || generation.Err() != nil {
		t.Fatal("explicit Open did not start a fresh gateway generation")
	}
	// Exercise both an old sleeping retry and a delayed error/control packet
	// from the closed socket after the replacement has been established.
	s.reconnect(fixture.generation)
	s.reconnectConnection(fixture.connection, websocket.CloseServiceRestart, true)
	select {
	case <-fixture.done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("old reconnect generation did not finish")
	}
	s.RLock()
	unchanged := s.wsConn == connection && s.sessionID == sessionID
	s.RUnlock()
	if !unchanged {
		t.Fatal("stale gateway work closed or invalidated the new connection")
	}
	if err := s.VoiceStateUpdate("guild", "", false, false); err != nil {
		t.Fatalf("replacement gateway is unusable: %v", err)
	}
	if count := fixture.attempts.Load(); count != 3 {
		t.Fatalf("dial attempts = %d, want 3", count)
	}
}

func TestGatewayControlPacketDuringHandshake(t *testing.T) {
	for _, test := range []struct {
		name       string
		packet     string
		invalidate bool
	}{
		{name: "reconnect", packet: `{"op":7}`},
		{name: "invalid session", packet: `{"op":9,"d":false}`, invalidate: true},
		{name: "resumable session", packet: `{"op":9,"d":true}`},
		{name: "unexpected ack", packet: `{"op":11}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newGatewayTestSession(t, func(connection *websocket.Conn) {
				if err := sendGatewayHello(connection); err != nil {
					return
				}
				if _, _, err := connection.ReadMessage(); err != nil {
					return
				}
				_ = connection.WriteMessage(websocket.TextMessage, []byte(test.packet))
			})
			s.sessionID = "previous session"
			atomic.StoreInt64(s.sequence, 1)
			s.resumeGatewayURL = s.gateway
			done := make(chan error, 1)
			go func() { done <- s.Open() }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("control packet unexpectedly completed handshake")
				}
			case <-time.After(time.Second):
				t.Fatal("gateway control packet deadlocked Open")
			}
			if test.invalidate && (s.sessionID != "" || s.resumeGatewayURL != "" || atomic.LoadInt64(s.sequence) != 0) {
				t.Fatal("invalid session retained its resume information")
			}
		})
	}
}

func TestGatewayReconnectIdentifiesAfterThreeFailedResumes(t *testing.T) {
	operations := make(chan int, 8)
	s := newGatewayTestSession(t, func(connection *websocket.Conn) {
		if err := sendGatewayHello(connection); err != nil {
			return
		}
		_, data, err := connection.ReadMessage()
		if err != nil {
			return
		}
		var packet struct {
			Operation int `json:"op"`
		}
		if err := json.Unmarshal(data, &packet); err != nil {
			return
		}
		operations <- packet.Operation
		if packet.Operation == 6 {
			// A gateway may keep rejecting apparently resumable state. Preserve
			// upstream's escape to a fresh identify after three failed resumes.
			_ = connection.WriteMessage(websocket.TextMessage, []byte(`{"op":7}`))
			return
		}
		if err := sendGatewayReady(connection); err != nil {
			return
		}
		for {
			if _, _, err := connection.ReadMessage(); err != nil {
				return
			}
		}
	})
	s.ShouldReconnectOnError = true
	s.sessionID = "stale session"
	s.resumeGatewayURL = s.gateway
	atomic.StoreInt64(s.sequence, 42)
	s.gatewayContext, s.gatewayCancel = context.WithCancel(context.Background())
	defer s.gatewayCancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.reconnect(s.gatewayContext)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	for _, expected := range []int{6, 6, 6, 2} {
		select {
		case actual := <-operations:
			if actual != expected {
				t.Fatalf("authentication opcode = %d, want %d", actual, expected)
			}
		case <-ctx.Done():
			t.Fatalf("reconnect did not send authentication opcode %d", expected)
		}
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("reconnect did not recover after identifying")
	}
	s.RLock()
	connected := s.wsConn != nil && s.sessionID == "session"
	s.RUnlock()
	if !connected {
		t.Fatal("fresh identify did not establish a usable session")
	}
}
