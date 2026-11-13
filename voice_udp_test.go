package discordgo

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

func TestVoiceUDPResolutionFailureReleasesConnectionLock(t *testing.T) {
	s, gateway := newVoiceTestSession(t)
	voice := newVoiceTestConnection(s)
	voice.op2 = voiceOP2{IP: "127.0.0.1", Port: 65536}
	err := voice.udpOpen(context.Background())
	if err == nil {
		t.Fatal("UDP resolution accepted an invalid port")
	}
	// failure needs the same lock to mark Dead and release voice waiters.
	// An early return from udpOpen must leave that teardown path usable.
	done := make(chan struct{})
	go func() {
		defer close(done)
		voice.failure(context.Background(), err)
	}()
	awaitVoiceTest(t, done, "UDP failure to finish connection teardown")
	if !errors.Is(voice.Err, err) || voice.Status != VoiceConnectionStatusDead {
		t.Fatalf("UDP failure left status %d and error %v", voice.Status, voice.Err)
	}
	select {
	case <-voice.DeadChannel():
	default:
		t.Fatal("UDP failure did not release Dead-channel waiters")
	}
	if leaves := gateway.leaves(t, s); leaves != 1 {
		t.Fatalf("UDP failure sent %d gateway leaves, want 1", leaves)
	}
	voice.Kill()
}

// newVoiceUDPTestServer is a stand-in for a voice server's UDP socket, which
// the connection under test is pointed at.
func newVoiceUDPTestServer(t *testing.T, voice *VoiceConnection) *net.UDPConn {
	t.Helper()
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	voice.op2 = voiceOP2{IP: "127.0.0.1", Port: server.LocalAddr().(*net.UDPAddr).Port}
	return server
}

func setVoiceUDPDiscoveryTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	previous := voiceUDPDiscoveryTimeout
	voiceUDPDiscoveryTimeout = timeout
	t.Cleanup(func() { voiceUDPDiscoveryTimeout = previous })
}

// IP discovery is one datagram each way. Losing either used to leave the
// handshake, and the websocket loop running it, waiting indefinitely.
func TestVoiceUDPDiscoveryGivesUpWithoutAnAnswer(t *testing.T) {
	setVoiceUDPDiscoveryTimeout(t, 20*time.Millisecond)
	s, _ := newVoiceTestSession(t)
	voice := newVoiceTestConnection(s)
	newVoiceUDPTestServer(t, voice) // receives the request and never answers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- voice.udpOpen(ctx) }()
	select {
	case err := <-result:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("unanswered discovery returned %v, want a deadline error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unanswered discovery never gave up")
	}
}

// The discovery deadline must not outlive discovery: the receive pump reads
// the same socket for the rest of the attempt, however long audio is silent.
func TestVoiceUDPDiscoveryLeavesSocketWithoutDeadline(t *testing.T) {
	const timeout = 50 * time.Millisecond
	setVoiceUDPDiscoveryTimeout(t, timeout)
	s, _ := newVoiceTestSession(t)
	voice := newVoiceTestConnection(s)
	server := newVoiceUDPTestServer(t, voice)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clients := make(chan *net.UDPAddr, 1)
	go func() {
		request := make([]byte, 74)
		_, client, err := server.ReadFromUDP(request)
		if err != nil {
			return
		}
		response := make([]byte, 74)
		binary.BigEndian.PutUint16(response, 2)
		binary.BigEndian.PutUint16(response[2:], 70)
		copy(response[8:], "127.0.0.1")
		binary.BigEndian.PutUint16(response[72:], uint16(client.Port))
		_, _ = server.WriteToUDP(response, client)
		clients <- client
	}()
	if err := voice.udpOpen(ctx); err != nil {
		t.Fatalf("answered discovery failed: %v", err)
	}
	voice.Cond.L.Lock()
	udpConn := voice.udpConn
	voice.Cond.L.Unlock()

	// An expired deadline fails a read at once, even with data waiting.
	time.Sleep(2 * timeout)
	if _, err := server.WriteToUDP([]byte("audio"), <-clients); err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	go func() {
		_, err := udpConn.Read(make([]byte, 16))
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("read after discovery failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read after discovery never received the datagram")
	}
}
