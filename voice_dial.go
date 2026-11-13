package discordgo

import (
	"context"
	"net"

	"github.com/gorilla/websocket"
)

// dialVoiceWebsocket interrupts the entire handshake when an attempt is
// cancelled. Gorilla v1.5.3's DialContext observes cancellation during TCP/TLS
// setup, but its HTTP upgrade response read only observes a socket deadline.
func dialVoiceWebsocket(
	ctx context.Context,
	dialer *websocket.Dialer,
	endpoint string,
) (*websocket.Conn, error) {
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	d := *dialer
	var stops []func() bool
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	watch := func(conn net.Conn, err error) (net.Conn, error) {
		if err != nil {
			return conn, err
		}
		// Observe the attempt context, rather than Gorilla's handshake
		// context, which it cancels even after a successful upgrade.
		stops = append(stops, context.AfterFunc(ctx, func() { _ = conn.Close() }))
		return conn, nil
	}
	netDialContext := d.NetDialContext
	netDial := d.NetDial
	d.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if netDialContext != nil {
			return watch(netDialContext(ctx, network, address))
		}
		if netDial != nil {
			return watch(netDial(network, address))
		}
		return watch((&net.Dialer{}).DialContext(ctx, network, address))
	}
	if netDialTLSContext := d.NetDialTLSContext; netDialTLSContext != nil {
		d.NetDialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			return watch(netDialTLSContext(ctx, network, address))
		}
	}
	conn, _, err := d.DialContext(ctx, endpoint, nil)
	return conn, err
}
