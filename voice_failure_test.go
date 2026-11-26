package discordgo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// voiceTestGateway is a stand-in for the main gateway that records the voice
// state updates (op 4) a session sends: the channel ID for a join, "" for a
// leave.
type voiceTestGateway struct {
	mu      sync.Mutex
	updates []string
	changed chan struct{}
}

func (g *voiceTestGateway) record(channelID string) {
	g.mu.Lock()
	g.updates = append(g.updates, channelID)
	g.mu.Unlock()
	select {
	case g.changed <- struct{}{}:
	default:
	}
}

func (g *voiceTestGateway) snapshot() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.updates...)
}

// waitFor blocks until the gateway has recorded an update for channelID.
func (g *voiceTestGateway) waitFor(t *testing.T, channelID string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		for _, update := range g.snapshot() {
			if update == channelID {
				return
			}
		}
		select {
		case <-g.changed:
		case <-deadline:
			t.Fatalf("gateway never saw a voice state update for %q; saw %q", channelID, g.snapshot())
		}
	}
}

// leaves reports how many leaves the gateway received. The gateway socket is
// ordered, so a marker update sent now is recorded after every update written
// before it; waiting for the marker makes the count exact rather than a guess
// about how long a stray write might take to arrive.
func (g *voiceTestGateway) leaves(t *testing.T, s *Session) int {
	t.Helper()
	const marker = "sync-marker"
	if err := s.voiceStateUpdate(context.Background(), "guild", marker, false, false); err != nil {
		t.Fatal(err)
	}
	g.waitFor(t, marker)
	count := 0
	for _, update := range g.snapshot() {
		if update == "" {
			count++
		}
	}
	return count
}

// leavesAfterDeath is leaves for a connection that has just been seen to die
// of a failure. Such a connection asks to leave once it is dead, so its leave
// may still be on the way, and counting at once would let the count's own
// marker reach the gateway first.
func (g *voiceTestGateway) leavesAfterDeath(t *testing.T, s *Session) int {
	t.Helper()
	g.waitFor(t, "")
	return g.leaves(t, s)
}

func newVoiceTestSession(t *testing.T) (*Session, *voiceTestGateway) {
	t.Helper()
	gateway := &voiceTestGateway{changed: make(chan struct{}, 1)}
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		connection, err := upgrader.Upgrade(resp, req, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		for {
			_, message, err := connection.ReadMessage()
			if err != nil {
				return
			}
			var update struct {
				Op   int `json:"op"`
				Data struct {
					ChannelID *string `json:"channel_id"`
				} `json:"d"`
			}
			if json.Unmarshal(message, &update) != nil || update.Op != 4 {
				continue
			}
			if update.Data.ChannelID == nil {
				gateway.record("")
			} else {
				gateway.record(*update.Data.ChannelID)
			}
		}
	}))
	t.Cleanup(server.Close)
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	s := &Session{
		wsConn:           connection,
		State:            NewState(),
		VoiceConnections: make(map[string]*VoiceConnection),
		LogLevel:         -1,
	}
	s.State.User = &User{ID: "1"}
	return s, gateway
}

// voiceTestEndpoint is a stand-in for a voice server. Each websocket
// handshake announces itself on dialing and then waits for release, so a test
// can hold a dial in flight for as long as it needs to.
type voiceTestEndpoint struct {
	address string
	dialing chan struct{}
	release chan struct{}
	// proceed lets every held handshake, present and future, complete.
	proceed func()
	// identified receives the opcode of the first message of each connection.
	identified chan int
}

func newVoiceTestEndpoint(t *testing.T, s *Session) *voiceTestEndpoint {
	t.Helper()
	endpoint := &voiceTestEndpoint{
		dialing:    make(chan struct{}, 8),
		release:    make(chan struct{}),
		identified: make(chan int, 8),
	}
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		endpoint.dialing <- struct{}{}
		<-endpoint.release
		connection, err := upgrader.Upgrade(resp, req, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		first := true
		for {
			_, message, err := connection.ReadMessage()
			if err != nil {
				return
			}
			if first {
				first = false
				var op struct {
					Op int `json:"op"`
				}
				if json.Unmarshal(message, &op) == nil {
					endpoint.identified <- op.Op
				}
			}
		}
	}))
	var releaseOnce sync.Once
	endpoint.proceed = func() { releaseOnce.Do(func() { close(endpoint.release) }) }
	// Handlers blocked on release must be let go before the server can close.
	t.Cleanup(server.Close)
	t.Cleanup(endpoint.proceed)
	endpoint.address = strings.TrimPrefix(server.URL, "https://")
	s.Dialer = &websocket.Dialer{
		TLSClientConfig: server.Client().Transport.(*http.Transport).TLSClientConfig,
	}
	return endpoint
}

// newVoiceTestConnection returns a registered connection that already knows
// its session ID, as it would after the gateway confirmed the join.
func newVoiceTestConnection(s *Session) *VoiceConnection {
	voice := newWebsocketTestVoice(s)
	voice.Status = VoiceConnectionStatusNew
	voice.sessionID = "session"
	voice.LogLevel = -1
	return voice
}

func awaitVoiceTest(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// A connection killed while its voice websocket handshake is in flight used
// to let the handshake finish, fail to identify on its cancelled context, and
// report that as a failure, which sent the gateway a second leave. That leave
// arrived after whatever join came next and evicted it.
func TestVoiceKillDuringDialSendsNoLeave(t *testing.T) {
	s, gateway := newVoiceTestSession(t)
	endpoint := newVoiceTestEndpoint(t, s)
	voice := newVoiceTestConnection(s)

	done := make(chan struct{})
	go func() {
		defer close(done)
		voice.websocket(context.Background(), endpoint.address, "token")
	}()
	awaitVoiceTest(t, endpoint.dialing, "the voice handshake to start")

	voice.Kill()
	awaitVoiceTest(t, done, "the killed dial to stop before its handshake is released")
	// Without cancellation the abandoned handshake would complete now.
	endpoint.proceed()

	if leaves := gateway.leaves(t, s); leaves != 0 {
		t.Fatalf("killed connection sent %d gateway leaves, want none", leaves)
	}
	if voice.Err != nil {
		t.Fatalf("deliberate kill was recorded as a failure: %v", voice.Err)
	}
	select {
	case op := <-endpoint.identified:
		t.Fatalf("killed connection still sent op %d to the voice server", op)
	default:
	}
}

func TestKilledVoiceDialCannotLeaveReplacementChannel(t *testing.T) {
	s, gateway := newVoiceTestSession(t)
	endpoint := newVoiceTestEndpoint(t, s)
	voice := newVoiceTestConnection(s)
	if err := s.voiceStateUpdate(context.Background(), "guild", "chan-1", false, false); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		voice.websocket(context.Background(), endpoint.address, "token")
	}()
	awaitVoiceTest(t, endpoint.dialing, "the first channel's voice dial")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := voice.Disconnect(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unacknowledged leave = %v, want deadline exceeded", err)
	}
	voice.Kill()
	current := newVoiceTestConnection(s)
	defer current.Kill()
	if err := s.voiceStateUpdate(context.Background(), "guild", "chan-2", false, false); err != nil {
		t.Fatal(err)
	}
	endpoint.proceed()
	awaitVoiceTest(t, done, "the abandoned first dial to stop")
	if leaves := gateway.leaves(t, s); leaves != 1 {
		t.Fatalf("voice replacement received %d leaves, want the single deliberate leave", leaves)
	}
	want := []string{"chan-1", "", "chan-2", "sync-marker"}
	updates := gateway.snapshot()
	if len(updates) != len(want) {
		t.Fatalf("gateway updates = %q, want %q", updates, want)
	}
	for i, channel := range want {
		if updates[i] != channel {
			t.Fatalf("gateway updates = %q, want %q", updates, want)
		}
	}
	if s.VoiceConnections["guild"] != current {
		t.Fatal("abandoned dial removed the replacement connection")
	}
}

// A second voice server update can arrive while the first one's handshake is
// still in flight. The superseded attempt must step aside without taking the
// connection down with it.
func TestVoiceServerUpdateDuringDialKeepsConnection(t *testing.T) {
	s, gateway := newVoiceTestSession(t)
	endpoint := newVoiceTestEndpoint(t, s)
	voice := newVoiceTestConnection(s)

	update := &VoiceServerUpdate{GuildID: "guild", Token: "token", Endpoint: &endpoint.address}
	if err := voice.onVoiceServerUpdate(update); err != nil {
		t.Fatal(err)
	}
	awaitVoiceTest(t, endpoint.dialing, "the first voice handshake to start")
	if err := voice.onVoiceServerUpdate(update); err != nil {
		t.Fatal(err)
	}
	awaitVoiceTest(t, endpoint.dialing, "the second voice handshake to start")
	endpoint.proceed()

	select {
	case op := <-endpoint.identified:
		if op != 0 {
			t.Fatalf("replacement attempt sent op %d, want identify", op)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement attempt never identified")
	}
	select {
	case op := <-endpoint.identified:
		t.Fatalf("superseded attempt also sent op %d", op)
	case <-time.After(50 * time.Millisecond):
	}
	if leaves := gateway.leaves(t, s); leaves != 0 {
		t.Fatalf("superseded attempt sent %d gateway leaves, want none", leaves)
	}
	voice.Cond.L.Lock()
	status, err := voice.Status, voice.Err
	voice.Cond.L.Unlock()
	if status == VoiceConnectionStatusDead || err != nil {
		t.Fatalf("superseded attempt killed the connection: status %d, error %v", status, err)
	}
	voice.Kill()
}

func TestVoiceFailureFromCancelledWorkIsIgnored(t *testing.T) {
	s, gateway := newVoiceTestSession(t)
	voice := newVoiceTestConnection(s)
	voice.Status = VoiceConnectionStatusReady

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	voice.failure(ctx, errors.New("write on a socket closed by cancellation"))

	if voice.Status != VoiceConnectionStatusReady || voice.Err != nil {
		t.Fatalf("cancelled work failed the connection: status %d, error %v", voice.Status, voice.Err)
	}
	if s.VoiceConnections["guild"] != voice {
		t.Fatal("cancelled work removed the connection from its session")
	}
	if leaves := gateway.leaves(t, s); leaves != 0 {
		t.Fatalf("cancelled work sent %d gateway leaves, want none", leaves)
	}
}

func TestVoiceFailureLeavesGuildOnce(t *testing.T) {
	s, gateway := newVoiceTestSession(t)
	voice := newVoiceTestConnection(s)
	voice.Status = VoiceConnectionStatusReady
	cause := errors.New("udp write error")

	voice.failure(context.Background(), cause)

	if voice.Status != VoiceConnectionStatusDead || !errors.Is(voice.Err, cause) {
		t.Fatalf("failure left status %d, error %v", voice.Status, voice.Err)
	}
	select {
	case <-voice.DeadChannel():
	default:
		t.Fatal("failure did not close the Dead channel")
	}
	if len(s.VoiceConnections) != 0 {
		t.Fatal("failed connection stayed registered")
	}
	if leaves := gateway.leaves(t, s); leaves != 1 {
		t.Fatalf("failure sent %d gateway leaves, want 1", leaves)
	}

	// A second pump reporting the same breakage must not leave again.
	voice.failure(context.Background(), errors.New("udp read error"))
	if !errors.Is(voice.Err, cause) {
		t.Fatalf("later failure replaced the original cause with %v", voice.Err)
	}
	if leaves := gateway.leaves(t, s); leaves != 1 {
		t.Fatalf("repeated failure raised gateway leaves to %d, want 1", leaves)
	}
}

func TestVoiceFailureOfReplacedConnectionSendsNoLeave(t *testing.T) {
	s, gateway := newVoiceTestSession(t)
	stale := newVoiceTestConnection(s)
	stale.Status = VoiceConnectionStatusReady
	current := newVoiceTestConnection(s) // takes over the guild's slot
	current.Status = VoiceConnectionStatusReady

	stale.failure(context.Background(), errors.New("udp write error"))

	if stale.Status != VoiceConnectionStatusDead {
		t.Fatal("failure did not kill the stale connection")
	}
	if s.VoiceConnections["guild"] != current || current.Status != VoiceConnectionStatusReady {
		t.Fatal("stale connection's failure disturbed its replacement")
	}
	if leaves := gateway.leaves(t, s); leaves != 0 {
		t.Fatalf("stale connection sent %d gateway leaves, want none", leaves)
	}
	current.Kill()
}
