package discordgo

import (
	"context"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const websocketWriteTimeout = 5 * time.Second

// websocketMutex permits cancelled voice operations to stop waiting behind
// another writer. Its zero value is ready for use.
type websocketMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *websocketMutex) LockContext(ctx context.Context) error {
	m.once.Do(func() { m.token = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.token <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *websocketMutex) Lock() {
	_ = m.LockContext(context.Background())
}

func (m *websocketMutex) Unlock() {
	<-m.token
}

func withWebsocketWriter(
	ctx context.Context,
	mutex *websocketMutex,
	write func(context.Context) error,
) error {
	ctx, cancel := context.WithTimeout(ctx, websocketWriteTimeout)
	defer cancel()
	if err := mutex.LockContext(ctx); err != nil {
		return err
	}
	defer mutex.Unlock()
	return write(ctx)
}

// writeWebsocket runs with the connection's writer lock held. Cancellation
// updates the underlying socket deadline because Gorilla snapshots its write
// deadline before starting a write. Wait for that update before releasing the
// writer lock, so it cannot interrupt a later request.
func writeWebsocket(
	ctx context.Context,
	connection *websocket.Conn,
	write func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if connection == nil {
		return ErrWSNotFound
	}
	deadline, _ := ctx.Deadline()
	if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = connection.UnderlyingConn().SetWriteDeadline(time.Now())
		close(done)
	})
	err := write()
	if !stop() {
		<-done
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (s *Session) writeGatewayJSON(ctx context.Context, data interface{}) error {
	return withWebsocketWriter(ctx, &s.wsMutex, func(ctx context.Context) error {
		return writeWebsocket(ctx, s.wsConn, func() error {
			return s.wsConn.WriteJSON(data)
		})
	})
}

func writeWebsocketJSON(
	ctx context.Context,
	mutex *websocketMutex,
	connection *websocket.Conn,
	data interface{},
) error {
	return withWebsocketWriter(ctx, mutex, func(ctx context.Context) error {
		return writeWebsocket(ctx, connection, func() error {
			return connection.WriteJSON(data)
		})
	})
}

func writeWebsocketMessage(
	ctx context.Context,
	mutex *websocketMutex,
	connection *websocket.Conn,
	messageType int,
	data []byte,
) error {
	return withWebsocketWriter(ctx, mutex, func(ctx context.Context) error {
		return writeWebsocket(ctx, connection, func() error {
			return connection.WriteMessage(messageType, data)
		})
	})
}
