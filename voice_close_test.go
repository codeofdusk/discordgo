package discordgo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// newVoiceCloseTestEndpoint is a stand-in for a voice server that closes the
// first websocket with code once it has identified. Later websockets stay
// open, and each identify is announced on the returned channel.
func newVoiceCloseTestEndpoint(t *testing.T, s *Session, code int) (string, <-chan struct{}) {
	t.Helper()
	identified := make(chan struct{}, 4)
	var connections atomic.Int32
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(
		resp http.ResponseWriter,
		req *http.Request,
	) {
		first := connections.Add(1) == 1
		connection, err := upgrader.Upgrade(resp, req, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if _, _, err := connection.ReadMessage(); err != nil {
			return
		}
		identified <- struct{}{}
		if first {
			_ = connection.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(code, "disconnected"),
				time.Now().Add(time.Second),
			)
		}
		_, _, _ = connection.ReadMessage()
	}))
	t.Cleanup(server.Close)
	s.Dialer = &websocket.Dialer{
		TLSClientConfig: server.Client().Transport.(*http.Transport).TLSClientConfig,
	}
	return strings.TrimPrefix(server.URL, "https://"), identified
}

func setVoiceServerUpdateGrace(t *testing.T, grace time.Duration) {
	t.Helper()
	previous := voiceServerUpdateGrace
	voiceServerUpdateGrace = grace
	t.Cleanup(func() { voiceServerUpdateGrace = previous })
}

// awaitVoiceWebsocketClosed blocks until the connection has noticed that its
// websocket is gone, which it does before deciding what the close means.
func awaitVoiceWebsocketClosed(t *testing.T, voice *VoiceConnection, identified <-chan struct{}) {
	t.Helper()
	awaitVoiceTest(t, identified, "the voice websocket to identify")
	deadline := time.Now().Add(2 * time.Second)
	for {
		voice.Cond.L.Lock()
		closed := voice.wsConn == nil
		voice.Cond.L.Unlock()
		if closed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("voice connection never noticed its websocket close")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFinalVoiceCloseKillsConnection(t *testing.T) {
	for _, code := range []int{4017, 4021} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			// These codes must not wait for a voice server update.
			setVoiceServerUpdateGrace(t, time.Hour)
			s, gateway := newVoiceTestSession(t)
			voice := newVoiceTestConnection(s)
			endpoint, _ := newVoiceCloseTestEndpoint(t, s, code)

			done := make(chan struct{})
			go func() {
				defer close(done)
				voice.websocket(context.Background(), endpoint, "token")
			}()
			awaitVoiceTest(t, voice.DeadChannel(), "the final close to kill the connection")
			awaitVoiceTest(t, done, "the closed voice websocket to stop")

			var closeErr *websocket.CloseError
			if !errors.As(voice.Err, &closeErr) || closeErr.Code != code {
				t.Fatalf("connection error = %v, want close code %d", voice.Err, code)
			}
			if leaves := gateway.leaves(t, s); leaves != 1 {
				t.Fatalf("final close sent %d gateway leaves, want 1", leaves)
			}
		})
	}
}

// Discord closes the voice websocket with these codes both when the session
// user is gone from the channel and when it was only moved. Without a gateway
// event to tell the two apart, the connection must still end up Dead.
func TestVoiceDisconnectWithoutServerUpdateKillsConnection(t *testing.T) {
	for _, code := range []int{4014, 4022} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			setVoiceServerUpdateGrace(t, 20*time.Millisecond)
			s, gateway := newVoiceTestSession(t)
			voice := newVoiceTestConnection(s)
			endpoint, _ := newVoiceCloseTestEndpoint(t, s, code)

			done := make(chan struct{})
			go func() {
				defer close(done)
				voice.websocket(context.Background(), endpoint, "token")
			}()
			awaitVoiceTest(t, voice.DeadChannel(), "the unanswered disconnect to kill the connection")
			awaitVoiceTest(t, done, "the closed voice websocket to stop")

			var closeErr *websocket.CloseError
			if !errors.Is(voice.Err, ErrVoiceDisconnected) ||
				!errors.As(voice.Err, &closeErr) || closeErr.Code != code {
				t.Fatalf("connection error = %v, want ErrVoiceDisconnected with close code %d", voice.Err, code)
			}
			if leaves := gateway.leaves(t, s); leaves != 1 {
				t.Fatalf("unanswered disconnect sent %d gateway leaves, want 1", leaves)
			}
		})
	}
}

// A move closes the voice websocket and then delivers a voice server update.
// The connection must survive the gap and reconnect, not report its death.
func TestVoiceDisconnectFollowedByServerUpdateReconnects(t *testing.T) {
	for _, code := range []int{4014, 4022} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			setVoiceServerUpdateGrace(t, time.Hour)
			s, gateway := newVoiceTestSession(t)
			voice := newVoiceTestConnection(s)
			endpoint, identified := newVoiceCloseTestEndpoint(t, s, code)

			update := &VoiceServerUpdate{GuildID: "guild", Token: "token", Endpoint: &endpoint}
			if err := voice.onVoiceServerUpdate(update); err != nil {
				t.Fatal(err)
			}
			awaitVoiceWebsocketClosed(t, voice, identified)

			if err := voice.onVoiceServerUpdate(update); err != nil {
				t.Fatal(err)
			}
			awaitVoiceTest(t, identified, "the replacement voice websocket to identify")

			voice.Cond.L.Lock()
			status, err := voice.Status, voice.Err
			voice.Cond.L.Unlock()
			if status == VoiceConnectionStatusDead || err != nil {
				t.Fatalf("moved connection died: status %d, error %v", status, err)
			}
			if leaves := gateway.leaves(t, s); leaves != 0 {
				t.Fatalf("moved connection sent %d gateway leaves, want none", leaves)
			}
			voice.Kill()
		})
	}
}

// A kick closes the voice websocket and then delivers a voice state update,
// which kills the connection. That is a deliberate end, not a failure.
func TestVoiceDisconnectFollowedByKillIsNotAFailure(t *testing.T) {
	setVoiceServerUpdateGrace(t, time.Hour)
	s, gateway := newVoiceTestSession(t)
	voice := newVoiceTestConnection(s)
	endpoint, identified := newVoiceCloseTestEndpoint(t, s, 4014)

	done := make(chan struct{})
	go func() {
		defer close(done)
		voice.websocket(context.Background(), endpoint, "token")
	}()
	awaitVoiceWebsocketClosed(t, voice, identified)

	voice.Kill()
	awaitVoiceTest(t, done, "the killed voice websocket to stop waiting")

	if voice.Err != nil {
		t.Fatalf("kick was recorded as a failure: %v", voice.Err)
	}
	if leaves := gateway.leaves(t, s); leaves != 0 {
		t.Fatalf("kicked connection sent %d gateway leaves, want none", leaves)
	}
}
