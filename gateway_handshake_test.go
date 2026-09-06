package discordgo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func newGatewayTestSession(t *testing.T, handler func(*websocket.Conn)) *Session {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(cam http.ResponseWriter, req *http.Request) {
		connection, err := upgrader.Upgrade(cam, req, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		handler(connection)
	}))
	t.Cleanup(server.Close)
	s, err := New("Bot test")
	if err != nil {
		t.Fatal(err)
	}
	s.gateway = "ws" + strings.TrimPrefix(server.URL, "http")
	s.ShouldReconnectOnError = false
	s.LogLevel = -1
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func sendGatewayHello(connection *websocket.Conn) error {
	return connection.WriteMessage(websocket.TextMessage, []byte(`{"op":10,"d":{"heartbeat_interval":1000}}`))
}

func sendGatewayReady(connection *websocket.Conn) error {
	return connection.WriteMessage(websocket.TextMessage, []byte(`{"op":0,"t":"READY","s":1,"d":{"session_id":"session","user":{"id":"bot"}}}`))
}

func TestGatewayHandshakeTimeoutReleasesSession(t *testing.T) {
	for _, stage := range []string{"hello", "ready"} {
		t.Run(stage, func(t *testing.T) {
			s := newGatewayTestSession(t, func(connection *websocket.Conn) {
				if stage == "ready" {
					if err := sendGatewayHello(connection); err != nil {
						return
					}
				}
				for {
					if _, _, err := connection.ReadMessage(); err != nil {
						return
					}
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.open(ctx) }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("silent gateway unexpectedly completed its handshake")
				}
			case <-time.After(time.Second):
				t.Fatal("gateway handshake did not respect its deadline")
			}
			if s.wsConn != nil {
				t.Fatal("failed handshake retained its websocket")
			}
			go func() { done <- s.Close() }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Close remained blocked after the handshake deadline")
			}
		})
	}
}

func TestGatewayClearsHandshakeDeadline(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	sendEvent := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(sendEvent)
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
		<-release
		_ = connection.WriteMessage(websocket.TextMessage, []byte(`{"op":0,"t":"MESSAGE_CREATE","s":2,"d":{"id":"message"}}`))
		for {
			if _, _, err := connection.ReadMessage(); err != nil {
				return
			}
		}
	})
	received := make(chan struct{}, 1)
	s.AddHandler(func(_ *Session, _ *MessageCreate) { received <- struct{}{} })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.open(ctx); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	sendEvent()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("live gateway retained its handshake deadline or cancellation callback")
	}
}
