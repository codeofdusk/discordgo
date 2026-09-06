package discordgo

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestGatewayResumeReplaysDispatchesBeforeResumed(t *testing.T) {
	authentication := make(chan int, 1)
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
		authentication <- packet.Operation
		for _, event := range []string{
			`{"op":0,"t":"MESSAGE_CREATE","s":2,"d":{"id":"first"}}`,
			`{"op":0,"t":"MESSAGE_CREATE","s":3,"d":{"id":"second"}}`,
			`{"op":0,"t":"RESUMED","s":4,"d":{}}`,
		} {
			if err := connection.WriteMessage(websocket.TextMessage, []byte(event)); err != nil {
				return
			}
		}
		for {
			if _, _, err := connection.ReadMessage(); err != nil {
				return
			}
		}
	})
	s.sessionID = "existing session"
	atomic.StoreInt64(s.sequence, 1)
	s.SyncEvents = true
	events := make(chan string, 3)
	s.AddHandler(func(session *Session, event *MessageCreate) {
		// A replay callback may need the session lock even though it arrives
		// before RESUMED. It must not run inside Open's critical section.
		session.Lock()
		defer session.Unlock()
		events <- event.ID
	})
	s.AddHandler(func(_ *Session, _ *Resumed) { events <- "resumed" })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	opened := make(chan error, 1)
	go func() { opened <- s.open(ctx) }()
	select {
	case err := <-opened:
		if err != nil {
			t.Fatalf("resume rejected replay dispatch: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("resume blocked while handling replay")
	}
	select {
	case operation := <-authentication:
		if operation != 6 {
			t.Fatalf("authentication opcode = %d, want RESUME (6)", operation)
		}
	case <-ctx.Done():
		t.Fatal("gateway did not receive authentication")
	}
	for _, expected := range []string{"first", "second", "resumed"} {
		select {
		case actual := <-events:
			if actual != expected {
				t.Fatalf("received event %q, want %q", actual, expected)
			}
		case <-ctx.Done():
			t.Fatalf("replay did not deliver %q", expected)
		}
	}
	if sequence := atomic.LoadInt64(s.sequence); sequence != 4 {
		t.Fatalf("resume sequence = %d, want 4", sequence)
	}
}
