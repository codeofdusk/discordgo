package discordgo

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"
)

func TestVoiceSendOpusFrameKeepsLatestOwnedAudio(t *testing.T) {
	v := &VoiceConnection{
		Cond:     sync.NewCond(&sync.Mutex{}),
		OpusSend: make(chan []byte, opusSendQueue),
	}
	const sent = opusSendQueue + 5
	for i := byte(1); i <= sent; i++ {
		frame := []byte{i}
		if !v.SendOpusFrame(frame) {
			t.Fatal("audio queue unexpectedly rejected a frame")
		}
		frame[0] = 0
	}
	for want := byte(sent - opusSendQueue + 1); want <= sent; want++ {
		if got := <-v.OpusSend; !bytes.Equal(got, []byte{want}) {
			t.Fatalf("queued audio = %v, want owned frame [%d]: the newest frames, oldest first", got, want)
		}
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

// newOpusSenderTest starts a sender that finds the given frames queued.
func newOpusSenderTest(t *testing.T, queued ...byte) *opusSenderTest {
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
	voice.OpusSend = make(chan []byte, opusSendQueue)
	voice.cipher = testAEAD{}
	voice.speaking = true
	st := &opusSenderTest{
		t:      t,
		voice:  voice,
		server: server,
		ticks:  make(chan time.Time),
		now:    time.Unix(1000, 0),
	}
	for _, frame := range queued {
		st.queue(frame)
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

// awaitQueued waits for the sender to leave exactly n frames queued.
func (st *opusSenderTest) awaitQueued(n int) {
	st.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(st.voice.OpusSend) != n {
		if time.Now().After(deadline) {
			st.t.Fatalf("%d frames queued, want %d", len(st.voice.OpusSend), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func (st *opusSenderTest) noPacket() {
	st.t.Helper()
	_ = st.server.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if n, err := st.server.Read(make([]byte, 1500)); err == nil {
		st.t.Fatalf("sender sent a %d byte packet before its tick", n)
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

// A call bridged from the telephone network delivers its audio unevenly: a
// packet that was late is followed at once by the ones held up behind it. A
// sender with room for one waiting frame dropped the rest of each burst.
func TestVoiceSessionSendsEveryFrameOfABurst(t *testing.T) {
	s, _ := newVoiceTestSession(t)
	voice := newVoiceTestConnection(s)
	server := newVoiceUDPTestServer(t, voice)
	conn, err := net.DialUDP("udp4", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	voice.udpConn = conn
	voice.deaf = true
	voice.speaking = true
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		voice.Kill()
		_ = conn.Close()
	})

	key, err := json.Marshal(voiceOP4{SecretKey: make([]byte, 32), Mode: "aead_aes256_gcm_rtpsize"})
	if err != nil {
		t.Fatal(err)
	}
	description, err := json.Marshal(voiceWebsocketMessage{Operation: 4, RawData: key})
	if err != nil {
		t.Fatal(err)
	}
	voice.onEvent(ctx, false, description)

	const burst = 6
	for i := byte(0); i < burst; i++ {
		if !voice.SendOpusFrame([]byte{i}) {
			t.Fatalf("session refused frame %d", i)
		}
	}
	buf := make([]byte, 1500)
	for want := uint16(0); want < burst; want++ {
		_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := server.Read(buf)
		if err != nil {
			t.Fatalf("frame %d of the burst was not sent: %v", want, err)
		}
		if got := binary.BigEndian.Uint16(buf[2:4]); n < 12 || got != want {
			t.Fatalf("packet %d has sequence %d and %d bytes", want, got, n)
		}
	}
}

// Frames left waiting behind one another are delay the listener hears, and
// once queued they would wait for the rest of the call.
func TestVoiceSenderWorksOffAStandingQueue(t *testing.T) {
	const standing = 2
	st := newOpusSenderTest(t, 1, 2)
	next := byte(standing)
	// A steady stream replaces each frame the sender takes.
	refill := func(taken int) {
		t.Helper()
		st.awaitQueued(standing - taken)
		for ; taken > 0; taken-- {
			next++
			st.queue(next)
		}
	}
	var packets []opusTestPacket
	ticked := func() {
		t.Helper()
		st.advance(20 * time.Millisecond)
		st.tick()
		packets = append(packets, st.packet())
	}

	// Every frame finds another waiting behind it. The sender keeps to its
	// ticks until that has gone on for opusBacklogFrames frames.
	for frame := 1; frame < opusBacklogFrames; frame++ {
		refill(1)
		if frame == opusBacklogFrames-1 {
			st.noPacket()
		}
		ticked()
	}
	// The next frame goes out ahead of its turn, on no tick.
	packets = append(packets, st.packet())
	// The sender went on to take the one after, which waits for its turn.
	refill(2)
	st.noPacket()
	ticked()
	// The frame sent ahead took up no time of its own, so an interval that
	// passes with nothing sent still shows.
	refill(1)
	st.advance(20 * time.Millisecond)
	ticked()

	for i, p := range packets {
		if !bytes.Equal(p.opus, []byte{byte(i + 1)}) {
			t.Fatalf("packet %d carries frame %v", i, p.opus)
		}
		want := packets[0].timestamp + uint32(i)*960
		if i == len(packets)-1 {
			want += 960
		}
		if p.timestamp != want {
			t.Errorf("packet %d is stamped %d, want %d", i, p.timestamp, want)
		}
	}
}

// A queue that keeps running empty is absorbing uneven arrivals rather than
// holding the audio up, however long that goes on.
func TestVoiceSenderKeepsAQueueThatRunsEmpty(t *testing.T) {
	st := newOpusSenderTest(t)
	for pair := 0; pair < opusBacklogFrames+5; pair++ {
		st.queue(1)
		st.queue(2)
		for want := byte(1); want <= 2; want++ {
			st.advance(20 * time.Millisecond)
			st.tick()
			if p := st.packet(); !bytes.Equal(p.opus, []byte{want}) {
				t.Fatalf("pair %d: sent frame %v, want [%d]", pair, p.opus, want)
			}
		}
	}
}
