package discordgo

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// voiceTestServer is a stand-in for a voice server: the websocket endpoint a
// connection dials, and the UDP socket it then sends its audio to.
type voiceTestServer struct {
	t        *testing.T
	endpoint string
	udp      *net.UDPConn
	// sockets receives each websocket the connection opens, in order.
	sockets chan *voiceTestSocket
	// audio receives each RTP packet sent to the UDP socket.
	audio chan voiceTestAudio
	// discoveries counts IP discovery requests: one per UDP socket opened.
	discoveries atomic.Int32
}

type voiceTestAudio struct {
	from     string // the address the packet was sent from
	sequence uint16
}

// voiceTestSocket is the voice server's end of one websocket.
type voiceTestSocket struct {
	t    *testing.T
	conn *websocket.Conn
	// messages receives what the connection sends, apart from heartbeats.
	messages chan voiceTestMessage
	// heartbeats receives the sequence number each heartbeat acknowledges.
	heartbeats chan int
	// closed receives the close code the connection ended the websocket
	// with, or 1006 if it ended it without one.
	closed chan int
}

type voiceTestMessage struct {
	Op   int             `json:"op"`
	Data json.RawMessage `json:"d"`
}

func newVoiceTestServer(t *testing.T, s *Session) *voiceTestServer {
	t.Helper()
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = udp.Close() })
	vs := &voiceTestServer{
		t:       t,
		udp:     udp,
		sockets: make(chan *voiceTestSocket, 16),
		audio:   make(chan voiceTestAudio, 256),
	}
	go vs.serveUDP()

	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(resp, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		socket := &voiceTestSocket{
			t:          t,
			conn:       conn,
			messages:   make(chan voiceTestMessage, 64),
			heartbeats: make(chan int, 64),
			closed:     make(chan int, 1),
		}
		vs.sockets <- socket
		socket.read()
	}))
	t.Cleanup(server.Close)
	vs.endpoint = strings.TrimPrefix(server.URL, "https://")
	s.Dialer = &websocket.Dialer{
		TLSClientConfig: server.Client().Transport.(*http.Transport).TLSClientConfig,
	}
	return vs
}

// serveUDP answers IP discovery and reports the audio it is sent.
func (vs *voiceTestServer) serveUDP() {
	buf := make([]byte, 1500)
	for {
		n, from, err := vs.udp.ReadFromUDP(buf)
		if err != nil {
			return
		}
		switch {
		case n == 74 && binary.BigEndian.Uint16(buf) == 1:
			vs.discoveries.Add(1)
			response := make([]byte, 74)
			binary.BigEndian.PutUint16(response, 2)
			binary.BigEndian.PutUint16(response[2:], 70)
			copy(response[8:], "127.0.0.1")
			binary.BigEndian.PutUint16(response[72:], uint16(from.Port))
			_, _ = vs.udp.WriteToUDP(response, from)
		case n >= 12 && buf[0] == 0x80 && buf[1] == 0x78:
			vs.audio <- voiceTestAudio{from: from.String(), sequence: binary.BigEndian.Uint16(buf[2:4])}
		}
	}
}

// read sorts what the connection sends until the websocket ends.
func (socket *voiceTestSocket) read() {
	for {
		_, raw, err := socket.conn.ReadMessage()
		if err != nil {
			code := websocket.CloseAbnormalClosure
			var closed *websocket.CloseError
			if errors.As(err, &closed) {
				code = closed.Code
			}
			socket.closed <- code
			return
		}
		var message voiceTestMessage
		if err := json.Unmarshal(raw, &message); err != nil {
			continue
		}
		if message.Op == 3 {
			var heartbeat voiceHeartbeatData
			_ = json.Unmarshal(message.Data, &heartbeat)
			socket.heartbeats <- heartbeat.SeqAck
			continue
		}
		socket.messages <- message
	}
}

// accept returns the next websocket the connection opens.
func (vs *voiceTestServer) accept() *voiceTestSocket {
	vs.t.Helper()
	select {
	case socket := <-vs.sockets:
		return socket
	case <-time.After(2 * time.Second):
		vs.t.Fatal("the connection opened no voice websocket")
		return nil
	}
}

// packet returns the next audio packet the voice server received.
func (vs *voiceTestServer) packet() voiceTestAudio {
	vs.t.Helper()
	select {
	case packet := <-vs.audio:
		return packet
	case <-time.After(2 * time.Second):
		vs.t.Fatal("the voice server received no audio")
		return voiceTestAudio{}
	}
}

// expect returns the payload of the connection's next message, which must
// have the given opcode. Heartbeats are not counted.
func (socket *voiceTestSocket) expect(op int) json.RawMessage {
	socket.t.Helper()
	select {
	case message := <-socket.messages:
		if message.Op != op {
			socket.t.Fatalf("connection sent op %d (%s), want op %d", message.Op, message.Data, op)
		}
		return message.Data
	case <-time.After(2 * time.Second):
		socket.t.Fatalf("connection never sent op %d", op)
		return nil
	}
}

// send sends the connection a message, numbered unless seq is negative.
func (socket *voiceTestSocket) send(op, seq int, data interface{}) {
	socket.t.Helper()
	message := map[string]interface{}{"op": op, "d": data}
	if seq >= 0 {
		message["seq"] = seq
	}
	if err := socket.conn.WriteJSON(message); err != nil {
		socket.t.Fatalf("voice server could not send op %d: %v", op, err)
	}
}

// drop breaks the websocket off, as a network failure would.
func (socket *voiceTestSocket) drop() {
	_ = socket.conn.UnderlyingConn().Close()
}

// close ends the websocket with a close code, as the voice server does.
func (socket *voiceTestSocket) close(code int) {
	_ = socket.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(code, "closed by the voice server"),
		time.Now().Add(time.Second),
	)
}

// hello gives the websocket its heartbeat interval. The interval is long, so
// each websocket sends the one heartbeat that answers this.
func (socket *voiceTestSocket) hello() {
	socket.t.Helper()
	socket.send(8, -1, voiceOP8{HeartbeatInterval: 60000})
}

// setUp takes the session the connection starts on socket as far as its
// description, numbering the description seq, and returns its identify.
func (vs *voiceTestServer) setUp(socket *voiceTestSocket, seq int) json.RawMessage {
	vs.t.Helper()
	identify := socket.expect(0)
	socket.hello()
	socket.send(2, -1, voiceOP2{
		SSRC:  7,
		IP:    "127.0.0.1",
		Port:  vs.udp.LocalAddr().(*net.UDPAddr).Port,
		Modes: []string{"aead_aes256_gcm_rtpsize"},
	})
	// The connection selects its protocol once discovery has been answered.
	socket.expect(1)
	socket.send(4, seq, voiceOP4{SecretKey: make([]byte, 32), Mode: "aead_aes256_gcm_rtpsize"})
	return identify
}

// newVoiceSessionTest points a connection at a voice server stand-in, as a
// voice server update does once the gateway has confirmed a join.
func newVoiceSessionTest(t *testing.T) (*voiceTestServer, *VoiceConnection, *voiceTestGateway) {
	t.Helper()
	s, gateway := newVoiceTestSession(t)
	server := newVoiceTestServer(t, s)
	voice := newVoiceTestConnection(s)
	t.Cleanup(voice.Kill)
	update := &VoiceServerUpdate{GuildID: "guild", Token: "token", Endpoint: &server.endpoint}
	if err := voice.onVoiceServerUpdate(update); err != nil {
		t.Fatal(err)
	}
	return server, voice, gateway
}

func awaitVoiceReady(t *testing.T, voice *VoiceConnection) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := voice.waitUntilStatus(ctx, VoiceConnectionStatusReady); err != nil {
		t.Fatalf("connection did not become ready: %v", err)
	}
}

// awaitVoiceSeqAck waits for the connection to have handled the message
// numbered seq.
func awaitVoiceSeqAck(t *testing.T, voice *VoiceConnection, seq int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		voice.Cond.L.Lock()
		got := voice.seqAck
		voice.Cond.L.Unlock()
		if got == seq {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("connection last handled message %d, want %d", got, seq)
		case <-time.After(time.Millisecond):
		}
	}
}

func setVoiceResumeTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	previous := voiceResumeTimeout
	voiceResumeTimeout = timeout
	t.Cleanup(func() { voiceResumeTimeout = previous })
}

func setVoiceSocketRetryDelay(t *testing.T, delay time.Duration) {
	t.Helper()
	previous := voiceSocketRetryDelay
	voiceSocketRetryDelay = delay
	t.Cleanup(func() { voiceSocketRetryDelay = previous })
}

// sendVoiceTestFrame has the connection send one frame and returns the packet
// that carried it to the voice server.
func sendVoiceTestFrame(t *testing.T, server *voiceTestServer, voice *VoiceConnection, frame byte) voiceTestAudio {
	t.Helper()
	if !voice.SendOpusFrame([]byte{frame}) {
		t.Fatalf("connection refused frame %d", frame)
	}
	return server.packet()
}

// A voice websocket that breaks off leaves its session in place on the voice
// server. Resuming on a new websocket keeps everything the session
// negotiated. The resume used to open a new UDP socket and select the
// protocol again, and Discord ends a session that does: close 4006.
func TestLostVoiceWebsocketResumesItsSession(t *testing.T) {
	// A websocket that served its session is replaced at once.
	setVoiceSocketRetryDelay(t, time.Hour)
	server, voice, gateway := newVoiceSessionTest(t)
	first := server.accept()
	server.setUp(first, 1)
	awaitVoiceReady(t, voice)
	first.send(12, 5, map[string]interface{}{"user_id": "2", "audio_ssrc": 9})
	awaitVoiceSeqAck(t, voice, 5)

	before := sendVoiceTestFrame(t, server, voice, 1)
	first.expect(5)

	first.drop()
	second := server.accept()
	var resume struct {
		ServerID  string `json:"server_id"`
		SessionID string `json:"session_id"`
		Token     string `json:"token"`
		SeqAck    int    `json:"seq_ack"`
	}
	if err := json.Unmarshal(second.expect(7), &resume); err != nil {
		t.Fatal(err)
	}
	if resume.ServerID != "guild" || resume.SessionID != "session" || resume.Token != "token" || resume.SeqAck != 5 {
		t.Fatalf("resume = %+v, want the session's guild, session and token, acknowledging message 5", resume)
	}

	// The session's audio does not wait for the websocket.
	during := sendVoiceTestFrame(t, server, voice, 2)

	second.hello()
	second.send(9, -1, nil)
	awaitVoiceReady(t, voice)
	select {
	case seq := <-second.heartbeats:
		if seq != 5 {
			t.Fatalf("resumed websocket's heartbeat acknowledged message %d, want 5", seq)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resumed websocket never sent a heartbeat")
	}

	after := sendVoiceTestFrame(t, server, voice, 3)
	for i, packet := range []voiceTestAudio{before, during, after} {
		if packet.from != before.from || packet.sequence != before.sequence+uint16(i) {
			t.Fatalf("packet %d came from %s numbered %d: the session's audio did not carry on from %s, %d",
				i, packet.from, packet.sequence, before.from, before.sequence)
		}
	}
	// The first thing the connection says on the resumed websocket is that
	// it is speaking. It does not select a protocol again.
	second.expect(5)
	if discoveries := server.discoveries.Load(); discoveries != 1 {
		t.Fatalf("connection opened %d UDP sockets, want the session's one", discoveries)
	}
	if leaves := gateway.leaves(t, voice.session); leaves != 0 {
		t.Fatalf("resumed connection sent %d gateway leaves, want none", leaves)
	}
}

// The time allowed for a resume to be confirmed is not a limit on how long
// the resumed websocket may then go without hearing from the voice server.
func TestConfirmedVoiceResumeOutlastsItsTimeout(t *testing.T) {
	const timeout = 250 * time.Millisecond
	setVoiceResumeTimeout(t, timeout)
	server, voice, _ := newVoiceSessionTest(t)
	first := server.accept()
	server.setUp(first, 1)
	awaitVoiceReady(t, voice)

	first.drop()
	second := server.accept()
	second.expect(7)
	second.send(9, -1, nil)
	// The heartbeat answers the hello, which the connection handles after
	// the confirmation.
	second.hello()
	select {
	case <-second.heartbeats:
	case <-time.After(2 * time.Second):
		t.Fatal("resumed websocket never sent a heartbeat")
	}

	select {
	case code := <-second.closed:
		t.Fatalf("connection ended its resumed websocket with code %d", code)
	case <-voice.DeadChannel():
		t.Fatalf("resumed connection died: %v", voice.Err)
	case <-time.After(2 * timeout):
	}
	awaitVoiceReady(t, voice)
}

// A voice server that takes the websocket and never confirms the resume
// would otherwise hold the connection in a session that carries no audio.
func TestUnconfirmedVoiceResumeStartsANewSession(t *testing.T) {
	setVoiceResumeTimeout(t, 50*time.Millisecond)
	setVoiceSocketRetryDelay(t, time.Hour)
	server, voice, gateway := newVoiceSessionTest(t)
	first := server.accept()
	server.setUp(first, 1)
	awaitVoiceReady(t, voice)

	first.drop()
	second := server.accept()
	second.expect(7)
	second.hello()

	third := server.accept()
	server.setUp(third, 1)
	awaitVoiceReady(t, voice)
	select {
	case <-second.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection kept the websocket whose resume was never confirmed")
	}
	if leaves := gateway.leaves(t, voice.session); leaves != 0 {
		t.Fatalf("connection sent %d gateway leaves, want none", leaves)
	}
}

// awaitNoVoiceWebsocket fails if the connection opens another websocket.
func awaitNoVoiceWebsocket(t *testing.T, server *voiceTestServer, why string) {
	t.Helper()
	select {
	case <-server.sockets:
		t.Fatalf("connection opened another voice websocket %s", why)
	case <-time.After(50 * time.Millisecond):
	}
}

func voiceTestCloseCode(err error) int {
	var closed *websocket.CloseError
	if errors.As(err, &closed) {
		return closed.Code
	}
	return 0
}

// Discord ends a voice session with close code 4006 when it is no longer
// valid, which can happen while the connection keeps its place in the call.
// That code used to fail the connection, and the call with it.
func TestVoiceSessionThatDiscordEndsIsReplaced(t *testing.T) {
	// The voice server has said the session is over: nothing is waited for.
	setVoiceSocketRetryDelay(t, time.Hour)
	setVoiceServerUpdateGrace(t, time.Hour)
	server, voice, gateway := newVoiceSessionTest(t)
	first := server.accept()
	server.setUp(first, 1)
	awaitVoiceReady(t, voice)
	before := sendVoiceTestFrame(t, server, voice, 1)
	first.expect(5)

	first.close(4006)
	second := server.accept()
	var identify struct {
		SessionID string `json:"session_id"`
		Token     string `json:"token"`
	}
	if err := json.Unmarshal(server.setUp(second, 1), &identify); err != nil {
		t.Fatal(err)
	}
	if identify.SessionID != "session" || identify.Token != "token" {
		t.Fatalf("new session identified as %+v, want the voice server update's session and token", identify)
	}
	awaitVoiceReady(t, voice)

	// Nothing of the ended session carries over: the new one has its own
	// UDP socket and sender, and announces that it is speaking.
	after := sendVoiceTestFrame(t, server, voice, 2)
	if after.from == before.from || after.sequence != 0 {
		t.Fatalf("new session's first packet came from %s numbered %d; the ended session sent from %s",
			after.from, after.sequence, before.from)
	}
	second.expect(5)
	if leaves := gateway.leaves(t, voice.session); leaves != 0 {
		t.Fatalf("connection sent %d gateway leaves, want none", leaves)
	}
}

// A voice server may have dropped the session by the time its websocket is
// replaced. It answers the resume by closing the websocket.
func TestRefusedVoiceResumeStartsANewSession(t *testing.T) {
	setVoiceSocketRetryDelay(t, time.Hour)
	setVoiceServerUpdateGrace(t, time.Hour)
	server, voice, gateway := newVoiceSessionTest(t)
	first := server.accept()
	server.setUp(first, 1)
	awaitVoiceReady(t, voice)

	first.drop()
	second := server.accept()
	second.expect(7)
	second.close(4006)

	third := server.accept()
	server.setUp(third, 1)
	awaitVoiceReady(t, voice)
	sendVoiceTestFrame(t, server, voice, 1)
	third.expect(5)
	if leaves := gateway.leaves(t, voice.session); leaves != 0 {
		t.Fatalf("connection sent %d gateway leaves, want none", leaves)
	}
}

// Moving the session user to another channel changes its voice token, and
// the session it had cannot be used again. The new token comes in a voice
// server update, which can arrive after the session has ended.
func TestVoiceSessionEndedByAMoveAwaitsItsVoiceServerUpdate(t *testing.T) {
	setVoiceServerUpdateGrace(t, time.Hour)
	server, voice, gateway := newVoiceSessionTest(t)
	first := server.accept()
	server.setUp(first, 1)
	awaitVoiceReady(t, voice)

	first.close(4006)
	// The connection asks for a session on the details it has, which the
	// voice server turns down.
	second := server.accept()
	second.expect(0)
	second.close(4006)
	awaitNoVoiceWebsocket(t, server, "after the voice server turned its session down")

	moved := &VoiceServerUpdate{GuildID: "guild", Token: "moved", Endpoint: &server.endpoint}
	if err := voice.onVoiceServerUpdate(moved); err != nil {
		t.Fatal(err)
	}
	third := server.accept()
	var identify struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(server.setUp(third, 1), &identify); err != nil {
		t.Fatal(err)
	}
	if identify.Token != "moved" {
		t.Fatalf("moved connection identified with token %q, want the new voice server update's", identify.Token)
	}
	awaitVoiceReady(t, voice)
	sendVoiceTestFrame(t, server, voice, 1)
	third.expect(5)
	if leaves := gateway.leaves(t, voice.session); leaves != 0 {
		t.Fatalf("moved connection sent %d gateway leaves, want none", leaves)
	}
}

// A voice server that turns a session down from the start is not asked
// again: its details are no good. Without new ones the connection is over.
func TestVoiceSessionTurnedDownWithoutVoiceServerUpdateKillsConnection(t *testing.T) {
	setVoiceServerUpdateGrace(t, 20*time.Millisecond)
	server, voice, gateway := newVoiceSessionTest(t)
	first := server.accept()
	first.expect(0)
	first.close(4006)

	awaitVoiceTest(t, voice.DeadChannel(), "the session turned down to kill the connection")
	if code := voiceTestCloseCode(voice.Err); code != 4006 {
		t.Fatalf("connection error = %v, want close code 4006", voice.Err)
	}
	awaitNoVoiceWebsocket(t, server, "after the voice server turned its session down")
	if leaves := gateway.leaves(t, voice.session); leaves != 1 {
		t.Fatalf("failed connection sent %d gateway leaves, want 1", leaves)
	}
}

// A voice server that ends every session it starts is not asked for them
// indefinitely.
func TestVoiceServerThatEndsEverySessionKillsConnection(t *testing.T) {
	setVoiceServerUpdateGrace(t, 20*time.Millisecond)
	server, voice, gateway := newVoiceSessionTest(t)
	for session := 0; session < voiceSessionLimit; session++ {
		socket := server.accept()
		server.setUp(socket, 1)
		awaitVoiceReady(t, voice)
		socket.close(4006)
	}

	awaitVoiceTest(t, voice.DeadChannel(), "the last session's end to kill the connection")
	if code := voiceTestCloseCode(voice.Err); code != 4006 {
		t.Fatalf("connection error = %v, want close code 4006", voice.Err)
	}
	awaitNoVoiceWebsocket(t, server, "beyond its limit of sessions")
	if leaves := gateway.leaves(t, voice.session); leaves != 1 {
		t.Fatalf("failed connection sent %d gateway leaves, want 1", leaves)
	}
}

// A websocket lost before its session was described has nothing to resume:
// the voice server may not have started the session at all. The next one
// starts it again, after a pause that keeps a voice server which drops every
// websocket from being dialled in a tight loop.
func TestVoiceWebsocketLostDuringSetupStartsTheSessionAgain(t *testing.T) {
	const delay = 100 * time.Millisecond
	setVoiceSocketRetryDelay(t, delay)
	server, voice, _ := newVoiceSessionTest(t)
	first := server.accept()
	first.expect(0)
	first.hello()
	dropped := time.Now()
	first.drop()

	second := server.accept()
	if waited := time.Since(dropped); waited < delay {
		t.Fatalf("websocket was replaced after %v, want a pause of %v", waited, delay)
	}
	server.setUp(second, 1)
	awaitVoiceReady(t, voice)
	sendVoiceTestFrame(t, server, voice, 1)
	second.expect(5)
}

// A sender can take a frame in the moment its session ends. It must not go on
// to announce speaking: the announcement would be recorded for the session
// that follows, which then never makes its own.
func TestEndedVoiceSessionSenderDoesNotAnnounceSpeaking(t *testing.T) {
	s, _ := newVoiceTestSession(t)
	server := newVoiceTestServer(t, s)
	voice := newVoiceTestConnection(s)
	t.Cleanup(voice.Kill)
	conn, _, err := s.Dialer.Dial("wss://"+server.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	voice.wsConn = conn
	voice.cipher = testAEAD{}
	voice.OpusSend = make(chan []byte, 1)
	udp, err := net.DialUDP("udp4", nil, server.udp.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = udp.Close() })

	ended, end := context.WithCancel(context.Background())
	end()
	// Which ready case a select takes is random, so give it many chances.
	for i := 0; i < 40; i++ {
		select {
		case voice.OpusSend <- []byte{1}:
		default:
		}
		clock := opusClock{interval: 20 * time.Millisecond, samples: 960}
		voice.sendOpus(ended, udp, clock, make(chan time.Time), time.Now)
	}

	voice.Cond.L.Lock()
	speaking := voice.speaking
	voice.Cond.L.Unlock()
	if speaking {
		t.Fatal("the sender of an ended session announced speaking")
	}
}

// An announcement can be on its way to a session's websocket when the next
// session starts. It says nothing about the new session.
func TestSpeakingAnnouncedToThePreviousSessionDoesNotCount(t *testing.T) {
	s, _ := newVoiceTestSession(t)
	previous := newVoiceTestServer(t, s)
	conn, _, err := s.Dialer.Dial("wss://"+previous.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	// The next session's handshake is held, so its websocket is not yet the
	// connection's when the announcement lands.
	endpoint := newVoiceTestEndpoint(t, s)
	voice := newVoiceTestConnection(s)
	t.Cleanup(voice.Kill)
	voice.wsConn = conn

	// Hold the announcement at the point of writing.
	voice.wsMu.Lock()
	announced := make(chan error, 1)
	go func() { announced <- voice.Speaking(true) }()
	time.Sleep(20 * time.Millisecond)

	update := &VoiceServerUpdate{GuildID: "guild", Token: "token", Endpoint: &endpoint.address}
	if err := voice.onVoiceServerUpdate(update); err != nil {
		t.Fatal(err)
	}
	awaitVoiceTest(t, endpoint.dialing, "the next session's voice handshake to start")
	voice.wsMu.Unlock()

	select {
	case err := <-announced:
		if !errors.Is(err, ErrWSNotFound) {
			t.Fatalf("announcement to the previous session returned %v, want ErrWSNotFound", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("announcement never returned")
	}
	previous.accept().expect(5)
	voice.Cond.L.Lock()
	speaking := voice.speaking
	voice.Cond.L.Unlock()
	if speaking {
		t.Fatal("the new session counted an announcement made to the previous one")
	}
}

// A heartbeat belongs to the websocket whose hello set its interval. The
// session goes on to other websockets, and each of those has its own.
func TestVoiceHeartbeatStopsWithItsWebsocket(t *testing.T) {
	s, _ := newVoiceTestSession(t)
	server := newVoiceTestServer(t, s)
	voice := newVoiceTestConnection(s)
	t.Cleanup(voice.Kill)
	conn, _, err := s.Dialer.Dial("wss://"+server.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	accepted := server.accept()

	session, endSession := context.WithCancel(context.Background())
	defer endSession()
	ctx, closeSocket := context.WithCancel(session)
	defer closeSocket()
	hello, err := json.Marshal(map[string]interface{}{"op": 8, "d": voiceOP8{HeartbeatInterval: 5}})
	if err != nil {
		t.Fatal(err)
	}
	voice.onEvent(voiceSocket{session: session, ctx: ctx, conn: conn}, false, hello)
	for beats := 0; beats < 3; beats++ {
		select {
		case <-accepted.heartbeats:
		case <-time.After(2 * time.Second):
			t.Fatal("websocket stopped receiving heartbeats")
		}
	}

	closeSocket()
	// A heartbeat already on its way may still arrive.
	time.Sleep(50 * time.Millisecond)
	for len(accepted.heartbeats) > 0 {
		<-accepted.heartbeats
	}
	select {
	case <-accepted.heartbeats:
		t.Fatal("heartbeat outlived its websocket")
	case <-time.After(50 * time.Millisecond):
	}
}

// A voice server update that arrives while a session is running starts a new
// session on the details it brings, as a move to another channel does. The
// pumps of the session it replaces read their parameters when they start,
// and the new session's setup writes the same ones.
func TestVoiceServerUpdateReplacesARunningSession(t *testing.T) {
	server, voice, gateway := newVoiceSessionTest(t)
	first := server.accept()
	server.setUp(first, 1)
	awaitVoiceReady(t, voice)

	moved := &VoiceServerUpdate{GuildID: "guild", Token: "moved", Endpoint: &server.endpoint}
	if err := voice.onVoiceServerUpdate(moved); err != nil {
		t.Fatal(err)
	}
	second := server.accept()
	var identify struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(server.setUp(second, 1), &identify); err != nil {
		t.Fatal(err)
	}
	if identify.Token != "moved" {
		t.Fatalf("new session identified with token %q, want the voice server update's", identify.Token)
	}
	awaitVoiceReady(t, voice)

	if packet := sendVoiceTestFrame(t, server, voice, 1); packet.sequence != 0 {
		t.Fatalf("new session's first packet is numbered %d, want 0", packet.sequence)
	}
	second.expect(5)
	if discoveries := server.discoveries.Load(); discoveries != 2 {
		t.Fatalf("connection opened %d UDP sockets, want one for each session", discoveries)
	}
	select {
	case <-first.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection kept the websocket of the session it replaced")
	}
	if leaves := gateway.leaves(t, voice.session); leaves != 0 {
		t.Fatalf("connection sent %d gateway leaves, want none", leaves)
	}
}
