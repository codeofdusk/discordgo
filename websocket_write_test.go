package discordgo

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// stalledWebsocketConn uses an unread pipe to exercise a socket write that
// cannot progress. Handshake traffic still uses the local HTTP server.
type stalledWebsocketConn struct {
	net.Conn
	pipe    net.Conn
	mu      sync.Mutex
	stalled bool
	started chan struct{}
	once    sync.Once
}

func (c *stalledWebsocketConn) Write(data []byte) (int, error) {
	c.mu.Lock()
	stalled := c.stalled
	c.mu.Unlock()
	if stalled {
		c.once.Do(func() { close(c.started) })
		return c.pipe.Write(data)
	}
	return c.Conn.Write(data)
}

func (c *stalledWebsocketConn) SetWriteDeadline(deadline time.Time) error {
	if err := c.pipe.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return c.Conn.SetWriteDeadline(deadline)
}

func (c *stalledWebsocketConn) Close() error {
	_ = c.pipe.Close()
	return c.Conn.Close()
}

func newWebsocketTestSession(t *testing.T) (*Session, *stalledWebsocketConn) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(cam http.ResponseWriter, req *http.Request) {
		connection, err := upgrader.Upgrade(cam, req, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		for {
			if _, _, err := connection.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	pipe, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	var network *stalledWebsocketConn
	dialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, networkType, address string) (net.Conn, error) {
			connection, err := (&net.Dialer{}).DialContext(ctx, networkType, address)
			if err != nil {
				return nil, err
			}
			network = &stalledWebsocketConn{
				Conn:    connection,
				pipe:    pipe,
				started: make(chan struct{}),
			}
			return network, nil
		},
	}
	gateway, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gateway.Close() })
	return &Session{
		wsConn:           gateway,
		VoiceConnections: make(map[string]*VoiceConnection),
	}, network
}

func newWebsocketTestVoice(s *Session) *VoiceConnection {
	voice := &VoiceConnection{
		Cond:    sync.NewCond(&sync.Mutex{}),
		session: s,
		GuildID: "guild",
		Status:  VoiceConnectionStatusReady,
		dead:    make(chan struct{}),
	}
	voice.Dead = voice.dead
	s.VoiceConnections[voice.GuildID] = voice
	return voice
}

func TestVoiceDisconnectAfterGatewayClose(t *testing.T) {
	s, _ := newWebsocketTestSession(t)
	voice := newWebsocketTestVoice(s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := voice.Disconnect(context.Background()); !errors.Is(err, ErrWSNotFound) {
		t.Fatalf("Disconnect error = %v, want ErrWSNotFound", err)
	}
	voice.Kill()
	select {
	case <-voice.DeadChannel():
	default:
		t.Fatal("local fallback did not kill the voice connection")
	}
}

func TestChannelVoiceJoinWithoutGateway(t *testing.T) {
	s := &Session{VoiceConnections: make(map[string]*VoiceConnection)}
	voice, err := s.ChannelVoiceJoin(context.Background(), "guild", "channel", false, false)
	if !errors.Is(err, ErrWSNotFound) {
		t.Fatalf("ChannelVoiceJoin error = %v, want ErrWSNotFound", err)
	}
	voice.Kill()
}

func TestVoiceDisconnectCancelsBlockedWrite(t *testing.T) {
	s, network := newWebsocketTestSession(t)
	voice := newWebsocketTestVoice(s)
	network.mu.Lock()
	network.stalled = true
	network.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- voice.Disconnect(ctx) }()
	select {
	case <-network.started:
	case <-time.After(time.Second):
		t.Fatal("gateway write did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Disconnect error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled disconnect remained blocked in socket write")
	}
}

func TestVoiceDisconnectDeadlineIncludesWriterLock(t *testing.T) {
	s, _ := newWebsocketTestSession(t)
	voice := newWebsocketTestVoice(s)
	// Open and Close own the session lock, and another network operation may
	// own the writer lock. Neither may prevent cancellation of a disconnect.
	s.Lock()
	defer s.Unlock()
	s.wsMutex.Lock()
	defer s.wsMutex.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- voice.Disconnect(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Disconnect error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnect remained blocked on a gateway lock")
	}
}

func TestVoiceStateUpdateDuringGatewayClose(t *testing.T) {
	s, _ := newWebsocketTestSession(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			_ = s.VoiceStateUpdate("guild", "", true, true)
		}
	}()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("voice state writer did not exit after close")
	}
}
