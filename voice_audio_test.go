package discordgo

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

func TestVoiceSendOpusFrameKeepsLatestOwnedAudio(t *testing.T) {
	v := &VoiceConnection{
		Cond:     sync.NewCond(&sync.Mutex{}),
		OpusSend: make(chan []byte, 1),
	}
	for i := byte(1); i <= 20; i++ {
		frame := []byte{i}
		if !v.SendOpusFrame(frame) {
			t.Fatal("audio queue unexpectedly rejected a frame")
		}
		frame[0] = 0
	}
	if got := <-v.OpusSend; !bytes.Equal(got, []byte{20}) {
		t.Fatalf("queued audio = %v, want newest owned frame [20]", got)
	}
}

func TestVoiceKillDoesNotCloseProducerChannel(t *testing.T) {
	s, _ := newVoiceTestSession(t)
	v := newVoiceTestConnection(s)
	v.OpusSend = make(chan []byte, 1)
	v.OpusRecv = make(chan *Packet, 1)
	v.receivers.Add(1)
	v.Kill()
	if v.SendOpusFrame([]byte{1}) {
		t.Fatal("dead connection accepted audio")
	}
	// An existing caller may already have selected its send before death.
	v.OpusSend <- []byte{2}
	select {
	case _, ok := <-v.OpusRecv:
		if !ok {
			t.Fatal("receive channel closed before its producer exited")
		}
	default:
	}
	v.receivers.Done()
	select {
	case _, ok := <-v.OpusRecv:
		if ok {
			t.Fatal("receive channel remained open after producer exit")
		}
	case <-time.After(time.Second):
		t.Fatal("receive channel did not close")
	}
	if got := <-v.OpusSend; !bytes.Equal(got, []byte{2}) {
		t.Fatalf("producer channel = %v, want [2]", got)
	}
	select {
	case _, ok := <-v.OpusSend:
		if !ok {
			t.Fatal("fork closed the caller-owned send channel")
		}
	default:
	}
}

func TestVoiceSendOpusFrameConcurrentKill(t *testing.T) {
	s, _ := newVoiceTestSession(t)
	v := newVoiceTestConnection(s)
	v.OpusSend = make(chan []byte, 1)
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 1000; j++ {
				v.SendOpusFrame([]byte{1})
			}
		}()
	}
	v.Kill()
	workers.Wait()
}

// opusSenderTest runs a connection's sender against a local stand-in for the
// voice server. The test supplies the sender's ticks and its clock.
type opusSenderTest struct {
	t      *testing.T
	voice  *VoiceConnection
	server *net.UDPConn
	ticks  chan time.Time

	mu  sync.Mutex
	now time.Time
}

type opusTestPacket struct {
	sequence  uint16
	timestamp uint32
	opus      []byte
}

func newOpusSenderTest(t *testing.T) *opusSenderTest {
	t.Helper()
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp4", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	s, _ := newVoiceTestSession(t)
	voice := newVoiceTestConnection(s)
	voice.OpusSend = make(chan []byte, 1)
	voice.cipher = testAEAD{}
	voice.speaking = true
	st := &opusSenderTest{
		t:      t,
		voice:  voice,
		server: server,
		ticks:  make(chan time.Time),
		now:    time.Unix(1000, 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		clock := opusClock{interval: 20 * time.Millisecond, samples: 960}
		voice.sendOpus(ctx, conn, clock, st.ticks, st.clock)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
		_ = conn.Close()
		_ = server.Close()
		voice.Kill()
	})
	return st
}

func (st *opusSenderTest) clock() time.Time {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.now
}

func (st *opusSenderTest) advance(d time.Duration) {
	st.mu.Lock()
	st.now = st.now.Add(d)
	st.mu.Unlock()
}

func (st *opusSenderTest) queue(frame byte) {
	st.t.Helper()
	if !st.voice.SendOpusFrame([]byte{frame}) {
		st.t.Fatalf("connection refused frame %d", frame)
	}
}

// tick gives the sender its turn to send one frame.
func (st *opusSenderTest) tick() {
	st.t.Helper()
	select {
	case st.ticks <- time.Time{}:
	case <-time.After(2 * time.Second):
		st.t.Fatal("sender was not waiting for a tick")
	}
}

func (st *opusSenderTest) packet() opusTestPacket {
	st.t.Helper()
	buf := make([]byte, 1500)
	_ = st.server.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := st.server.Read(buf)
	if err != nil {
		st.t.Fatalf("sender sent no packet: %v", err)
	}
	if n < 12+4 {
		st.t.Fatalf("sender sent a %d byte packet", n)
	}
	// The header, then the frame as testAEAD leaves it, then the nonce.
	return opusTestPacket{
		sequence:  binary.BigEndian.Uint16(buf[2:4]),
		timestamp: binary.BigEndian.Uint32(buf[4:8]),
		opus:      append([]byte(nil), buf[12:n-4]...),
	}
}

// A receiver measures delay by comparing a packet's timestamp with when it
// arrived. Timestamps that only counted frames made each pause in the audio
// look like that much network delay.
func TestVoiceSenderTimestampsFollowRealTime(t *testing.T) {
	st := newOpusSenderTest(t)
	send := func(after time.Duration, frame byte) opusTestPacket {
		t.Helper()
		st.advance(after)
		st.queue(frame)
		st.tick()
		return st.packet()
	}
	first := send(0, 1)
	prompt := send(20*time.Millisecond, 2)
	late := send(35*time.Millisecond, 3)
	resumed := send(705*time.Millisecond, 4)

	for i, p := range []opusTestPacket{first, prompt, late, resumed} {
		if p.sequence != first.sequence+uint16(i) || !bytes.Equal(p.opus, []byte{byte(i + 1)}) {
			t.Fatalf("packet %d has sequence %d and frame %v", i, p.sequence, p.opus)
		}
	}
	if got := prompt.timestamp - first.timestamp; got != 960 {
		t.Errorf("a frame sent 20ms after the last is %d samples on, want 960", got)
	}
	if got := late.timestamp - prompt.timestamp; got != 960 {
		t.Errorf("a frame sent 15ms late is %d samples on, want 960: lateness is not a gap", got)
	}
	// 760ms after the first frame, 20ms of which is this frame's own.
	if got := resumed.timestamp - first.timestamp; got != 38*960 {
		t.Errorf("a frame sent 760ms after the first is %d samples on, want %d", got, 38*960)
	}
}

func TestOpusClockLeavesOutOnlyWholeIdleIntervals(t *testing.T) {
	start := time.Unix(1000, 0)
	at := func(ms int) time.Time { return start.Add(time.Duration(ms) * time.Millisecond) }
	tests := []struct {
		name  string
		sends []int // milliseconds after the first frame
		want  []uint32
	}{
		{"steady", []int{0, 20, 40}, []uint32{0, 960, 1920}},
		{"ahead of time", []int{0, 1, 2, 60}, []uint32{0, 960, 1920, 2880}},
		{"late within the interval", []int{0, 39, 59}, []uint32{0, 960, 1920}},
		{"one interval idle", []int{0, 40, 60}, []uint32{0, 1920, 2880}},
		{"idle, then late", []int{0, 59, 79}, []uint32{0, 1920, 2880}},
		{"long pause", []int{0, 20, 10020}, []uint32{0, 960, 501 * 960}},
	}
	for _, tt := range tests {
		clock := opusClock{interval: 20 * time.Millisecond, samples: 960}
		for i, ms := range tt.sends {
			if got := clock.stamp(at(ms)); got != tt.want[i] {
				t.Errorf("%s: frame at %dms stamped %d, want %d", tt.name, ms, got, tt.want[i])
			}
		}
	}
}
