// Discordgo - Discord bindings for Go
// Available at https://github.com/bwmarrin/discordgo

// Copyright 2015-2016 Bruce Marriner <bruce@sqls.net>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file contains code related to Discord voice suppport

package discordgo

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/chacha20poly1305"
)

// ------------------------------------------------------------------------------------------------
// Code related to both VoiceConnection Websocket and UDP connections.
// ------------------------------------------------------------------------------------------------

// VoiceConnectionStatus is status of VoiceConnection
// New -> Connecting <-> Ready
// any -> Dead
type VoiceConnectionStatus int

const (
	// VoiceConnectionStatusInvalid means status not specified, maybe bug?
	VoiceConnectionStatusInvalid VoiceConnectionStatus = iota
	// VoiceConnectionStatusNew means initiating connection
	VoiceConnectionStatusNew
	// VoiceConnectionStatusConnecting means connecting websocket and udp (includes reconnecting)
	VoiceConnectionStatusConnecting
	// VoiceConnectionStatusReady means ready to send/receive audio
	VoiceConnectionStatusReady
	// VoiceConnectionStatusDead means already dead(error or disconnected normally)
	VoiceConnectionStatusDead
)

// A VoiceConnection struct holds all the data and functions related to a Discord Voice Connection.
type VoiceConnection struct {
	Cond *sync.Cond

	// Status of this connection. Read only
	Status VoiceConnectionStatus

	// Closed if this VoiceConection status become Dead
	Dead <-chan struct{}
	dead chan struct{}

	// contains unrecoverable error
	// if not nil, Status should be Dead
	Err error

	LogLevel int
	GuildID  string

	deaf     bool
	mute     bool
	speaking bool

	// OpusSend accepts caller-owned audio. It stays open after death so a
	// concurrent producer can never panic; use DeadChannel to stop producing.
	OpusSend chan []byte
	OpusRecv chan *Packet // Chan for receiving opus audio, automatically closed after dead, DON'T CLOSE YOURSELF

	wsMu websocketMutex

	// can be nil, use only for send message
	// mostly this is available connection or nil, but rarely closed connection
	wsConn *websocket.Conn

	// calling this may close websocket and all related connection.
	wsCancel context.CancelFunc

	udpConn *net.UDPConn

	session *Session

	sessionID string

	op2 voiceOP2
	op4 voiceOP4

	cipher cipher.AEAD

	dave *DAVESession

	ssrcToUserID map[uint32]string

	pendingReWelcome bool

	voiceSpeakingUpdateHandlers []VoiceSpeakingUpdateHandler

	seqAck int // for heartbeat and resume

	// pumpsStarted records whether opusSender/opusReceiver are running for
	// the current voice session; reset when a session starts so a repeated
	// session description cannot double-start them. A session that has its
	// pumps has been described, which is what makes it resumable.
	pumpsStarted bool
	receivers    sync.WaitGroup
}

// DeadChannel exposes the close notification channel for callers that need a
// stable exported liveness signal without reflecting on VoiceConnection internals.
func (v *VoiceConnection) DeadChannel() <-chan struct{} {
	if v == nil {
		return nil
	}
	return v.Dead
}

// SendOpusFrame queues an owned copy of a frame without blocking. A full
// queue gives up its oldest frame to make room. It returns false when the
// connection cannot accept audio.
func (v *VoiceConnection) SendOpusFrame(opus []byte) bool {
	if v == nil || len(opus) == 0 {
		return false
	}
	if v.Cond != nil {
		v.Cond.L.Lock()
		defer v.Cond.L.Unlock()
	}
	if v.Status == VoiceConnectionStatusDead || v.OpusSend == nil {
		return false
	}
	frame := append([]byte(nil), opus...)
	select {
	case v.OpusSend <- frame:
		return true
	default:
	}
	select {
	case <-v.OpusSend:
	default:
	}
	select {
	case v.OpusSend <- frame:
		return true
	default:
		return false
	}
}

// VoiceSpeakingUpdateHandler type provides a function definition for the
// VoiceSpeakingUpdate event
type VoiceSpeakingUpdateHandler func(vc *VoiceConnection, vs *VoiceSpeakingUpdate)

// Speaking sends a speaking notification to Discord over the voice websocket.
// This must be sent as true prior to sending audio and should be set to false
// once finished sending audio.
// b : Send true if speaking, false if not.
func (v *VoiceConnection) Speaking(b bool) (err error) {

	v.log(LogDebug, "called (%t)", b)

	type voiceSpeakingData struct {
		Speaking bool `json:"speaking"`
		Delay    int  `json:"delay"`
	}

	type voiceSpeakingOp struct {
		Op   int               `json:"op"` // Always 5
		Data voiceSpeakingData `json:"d"`
	}

	// Tolerate a zero-value VoiceConnection (Cond is only set once a join
	// runs), like the pre-rewrite embedded-mutex version did.
	if v.Cond == nil {
		return fmt.Errorf("no VoiceConnection websocket")
	}

	v.Cond.L.Lock()
	wsConn := v.wsConn
	v.Cond.L.Unlock()
	data := voiceSpeakingOp{5, voiceSpeakingData{b, 0}}
	err = writeWebsocketJSON(context.Background(), &v.wsMu, wsConn, data)

	v.Cond.L.Lock()
	defer v.Cond.L.Unlock()
	if v.wsConn != wsConn || v.Status == VoiceConnectionStatusDead {
		return ErrWSNotFound
	}
	v.Cond.Broadcast()
	if err != nil {
		v.speaking = false
		v.log(LogError, "Speaking() write json error, %s", err)
		return
	}

	v.speaking = b

	return
}

func (v *VoiceConnection) WaitForDAVEReady(ctx context.Context) error {
	return v.waitFor(ctx, func() bool {
		return v.Status != VoiceConnectionStatusDead && (v.dave == nil || v.dave.CanEncrypt())
	})
}

// Disconnect requests disconnect from this voice channel and wait for disconencted
func (v *VoiceConnection) Disconnect(ctx context.Context) error {

	v.log(LogInformational, "called")

	err := v.session.voiceStateUpdate(ctx, v.GuildID, "", true, true)
	if err != nil {
		return err
	}

	return v.waitUntilStatus(ctx, VoiceConnectionStatusDead)
}

// Kill stop all goroutines related to this voice conection, remove self from Session, and set status to dead.
// NOTE: unlock before calling this
func (v *VoiceConnection) Kill() {

	v.log(LogInformational, "called")

	v.kill(nil)

	v.log(LogInformational, "done")
}

// kill moves the connection to Dead, recording cause as its error when this
// call makes the transition. It reports whether it made the transition, and
// whether the connection was still the session's connection for its guild.
func (v *VoiceConnection) kill(cause error) (died, registered bool) {
	// Leave the session's map before becoming observably Dead, so a join that
	// sees the death cannot find this connection there and reuse it.
	v.session.Lock()
	if v.session.VoiceConnections[v.GuildID] == v {
		delete(v.session.VoiceConnections, v.GuildID)
		registered = true
	}
	v.session.Unlock()
	v.Cond.L.Lock()
	defer v.Cond.L.Unlock()
	if v.wsCancel != nil {
		v.wsCancel()
	}
	if v.Status == VoiceConnectionStatusDead {
		return false, registered
	}
	if cause != nil && v.Err == nil {
		v.Err = cause
	}
	v.Status = VoiceConnectionStatusDead
	v.Cond.Broadcast()
	close(v.dead)
	recv := v.OpusRecv
	go func() {
		// UDP readers may still be unwinding cancellation. Only their exit,
		// not an arbitrary delay, makes closing the receive channel safe.
		v.receivers.Wait()
		if recv != nil {
			close(recv)
		}
	}()
	return true, registered
}

// AddHandler adds a Handler for VoiceSpeakingUpdate events.
func (v *VoiceConnection) AddHandler(h VoiceSpeakingUpdateHandler) {
	v.Cond.L.Lock()
	defer v.Cond.L.Unlock()

	v.voiceSpeakingUpdateHandlers = append(v.voiceSpeakingUpdateHandlers, h)
	v.Cond.Broadcast()
}

// VoiceSpeakingUpdate is a struct for a VoiceSpeakingUpdate event.
type VoiceSpeakingUpdate struct {
	UserID   string `json:"user_id"`
	SSRC     int    `json:"ssrc"`
	Speaking int    `json:"speaking"`
}

// ------------------------------------------------------------------------------------------------
// Unexported Internal Functions Below.
// ------------------------------------------------------------------------------------------------

// failure handles an unrecoverable error seen by work running under ctx.
// VoiceConnection should be unlocked before calling this.
//
// Work whose context is already cancelled was stopped on purpose: by Kill, by
// a newer voice server update, or by the reconnect loop replacing its own
// attempt. The errors such work then runs into (a write on a closed socket, an
// identify sent after its dial was abandoned) are consequences of that stop,
// not failures of the connection, so they are ignored here. Treating them as
// failures would kill whatever superseded the work and tell the gateway to
// leave the guild, evicting a connection that is perfectly healthy.
func (v *VoiceConnection) failure(ctx context.Context, err error) {
	if ctx.Err() != nil {
		v.log(LogDebug, "ignoring voice error from cancelled work, %v", err)
		return
	}
	v.log(LogError, "voice unrecoverable error, %v", err.Error())
	v.log(LogDebug, "voice struct: %#v\n", v)
	// kill owns the Dead transition and wakes liveness waiters; the receive
	// channel closes once its producers have stopped.
	died, registered := v.kill(err)
	if !died || !registered {
		// Either the connection was already dead, or the session no longer
		// considered it the guild's connection. Its voice state is not ours
		// to change.
		return
	}
	v.leaveAfterFailure()
}

// leaveAfterFailure asks the gateway to leave the guild's voice channel once
// this connection has failed, so the session user does not linger there
// without audio. It is skipped if a newer connection has since been
// registered for the guild: the leave would apply to that one.
func (v *VoiceConnection) leaveAfterFailure() {
	v.session.RLock()
	_, replaced := v.session.VoiceConnections[v.GuildID]
	v.session.RUnlock()
	if replaced {
		return
	}
	// voiceStateUpdate bounds the gateway write itself.
	err := v.session.voiceStateUpdate(context.Background(), v.GuildID, "", true, true)
	if err != nil {
		v.log(LogWarning, "could not leave voice after failure, %v", err)
	}
}

// voiceWebsocketMessage is basic message struct of voice websocket
type voiceWebsocketMessage struct {
	Operation int             `json:"op"`
	RawData   json.RawMessage `json:"d"`
	Sequence  *int            `json:"seq"`
}

// A voiceOP2 stores the data for the voice operation 2 websocket event
// which is sort of like the voice READY packet
type voiceOP2 struct {
	SSRC  uint32   `json:"ssrc"`
	Port  int      `json:"port"`
	Modes []string `json:"modes"`
	IP    string   `json:"ip"`
}

// A voiceOP4 stores the data for the voice operation 4 websocket event
// which provides us with the NaCl SecretBox encryption key
type voiceOP4 struct {
	SecretKey           []byte `json:"secret_key"`
	Mode                string `json:"mode"`
	DAVEProtocolVersion int    `json:"dave_protocol_version"`
}

// A voiceOP8 stores the data for the voice operation 8 websocket event HELLO
type voiceOP8 struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

// waitUntilStatus waits for connection to be in given VoiceConnectionStatus
// returns error if context timeout or VoiceConnection.Err
func (v *VoiceConnection) waitUntilStatus(ctx context.Context, status VoiceConnectionStatus) error {
	v.log(LogInformational, "called")
	return v.waitFor(ctx, func() bool { return v.Status == status })
}

// waitFor evaluates ready with Cond.L held. Taking the same lock in the
// cancellation callback prevents a lost wakeup between checking ctx.Err and
// entering Cond.Wait. The caller owns the wait; no orphan waiter survives it.
func (v *VoiceConnection) waitFor(ctx context.Context, ready func() bool) error {
	stop := context.AfterFunc(ctx, func() {
		v.Cond.L.Lock()
		v.Cond.Broadcast()
		v.Cond.L.Unlock()
	})
	defer stop()
	v.Cond.L.Lock()
	defer v.Cond.L.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ready() {
			return v.Err
		}
		if v.Status == VoiceConnectionStatusDead {
			if v.Err != nil {
				return v.Err
			}
			return ErrVoiceConnectionDead
		}
		v.Cond.Wait()
	}
}

// onVoiceServerUpdate handles a VOICE_SERVER_UPDATE event of main gateway.
// wait for VOICE_SERVER_UPDATE and open voice websocket connection.
func (v *VoiceConnection) onVoiceServerUpdate(ev *VoiceServerUpdate) (err error) {

	v.log(LogInformational, "called")

	v.Cond.L.Lock()
	defer v.Cond.L.Unlock()

	// Close a websocket if one is already open
	if v.wsCancel != nil {
		v.wsCancel()
	}

	// If no endpoint, just wait for next event
	if ev.Endpoint == nil {
		return
	}

	go v.websocket(context.TODO(), *ev.Endpoint, ev.Token)

	return
}

// ErrVoiceNoSessionID means timed out to receive voice Session ID
var ErrVoiceNoSessionID = errors.New("did not receive voice Session ID in time")

// ErrVoiceReconnectionLimit means reached a hard limit to reconnect
var ErrVoiceReconnectionLimit = errors.New("reconnection limit reached")

// ErrVoiceConnectionDead means the connection closed before it became ready.
var ErrVoiceConnectionDead = errors.New("voice connection is dead")

// ErrVoiceUnknownEncryptionMode means Discord requested encryption mode which is not supported
var ErrVoiceUnknownEncryptionMode = errors.New("unknown encryption mode")

// ErrVoiceDisconnected means Discord closed the voice websocket and no voice
// server update arrived to replace it.
var ErrVoiceDisconnected = errors.New("voice connection disconnected by Discord")

// ErrVoiceResumeUnconfirmed means a voice server accepted a websocket opened
// to resume a voice session and then never confirmed the resume.
var ErrVoiceResumeUnconfirmed = errors.New("voice server did not confirm the resume")

// voiceResumeTimeout bounds how long a websocket opened to resume a voice
// session waits for the voice server to confirm the resume.
var voiceResumeTimeout = 10 * time.Second

// voiceSocketRetryDelay is the pause before replacing a websocket that was
// lost without ever serving its session, so that a voice server which keeps
// dropping its websockets is not dialled in a tight loop.
var voiceSocketRetryDelay = time.Second

// voiceSocketAttempts bounds how many websockets one voice server update may
// open: the first, and one for each time its session is resumed or restarted.
const voiceSocketAttempts = 100

// voiceSessionLimit is how many voice sessions one voice server update may
// start. Discord can end a session while the connection keeps its place in
// the call, and the voice server's details are then still good for another.
// The limit keeps a voice server that ends every session it starts from
// being asked for them indefinitely.
const voiceSessionLimit = 3

// voiceCloseWait bounds the sending of the close frame that ends a voice
// session the connection has finished with.
const voiceCloseWait = time.Second

// voiceServerUpdateGrace bounds how long a connection whose websocket Discord
// closed waits for the voice server update that would reopen it. A move
// normally delivers one within a second.
var voiceServerUpdateGrace = 5 * time.Second

// awaitVoiceServerUpdate parks a websocket that Discord closed until ctx ends,
// which is how both a replacement websocket and a kill take over, and reports
// whether one did so within voiceServerUpdateGrace. endSession stops the
// closed websocket's session, whose pumps have nothing left to talk to.
func (v *VoiceConnection) awaitVoiceServerUpdate(ctx context.Context, endSession context.CancelFunc) bool {
	endSession()

	v.Cond.L.Lock()
	if v.Status != VoiceConnectionStatusDead {
		v.Status = VoiceConnectionStatusConnecting
		v.Cond.Broadcast()
	}
	v.Cond.L.Unlock()

	grace := time.NewTimer(voiceServerUpdateGrace)
	defer grace.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-grace.C:
		return false
	}
}

// voiceSocket is one websocket of a voice session. A session outlives the
// websockets it is resumed on, and so do its UDP socket and audio pumps.
type voiceSocket struct {
	session context.Context // ends with the voice session
	ctx     context.Context // ends with this websocket, and with its heartbeat
	conn    *websocket.Conn
}

// voiceSocketEnd says what the loss of a voice websocket means for the
// session it carried.
type voiceSocketEnd int

const (
	// voiceSocketLost means nothing ended the session: the websocket failed
	// underneath it, or the voice server reported its own crash.
	voiceSocketLost voiceSocketEnd = iota
	// voiceSocketSessionOver means the voice server ended the session, or
	// would not resume it, without saying that the connection has lost its
	// place in the call.
	voiceSocketSessionOver
	// voiceSocketDisconnected means Discord took the connection out of its
	// call. Only the gateway knows whether it has been given another.
	voiceSocketDisconnected
	// voiceSocketRefused means Discord will not have the connection in the
	// call, and nothing will reopen it.
	voiceSocketRefused
)

// voiceSocketEnded classifies the error that ended a voice websocket.
func voiceSocketEnded(err error) voiceSocketEnd {
	if errors.Is(err, ErrVoiceResumeUnconfirmed) {
		return voiceSocketSessionOver
	}
	var closed *websocket.CloseError
	if !errors.As(err, &closed) {
		// A network-level disruption: a timeout, a TCP reset and the like.
		return voiceSocketLost
	}
	switch closed.Code {
	case 4014, 4022:
		// 4014 indicates that the session user was disconnected or moved;
		// 4022 indicates that the call was terminated (e.g., channel deleted, voice server changed, call ended).
		return voiceSocketDisconnected
	case 4015, websocket.CloseAbnormalClosure:
		// 4015 indicates that the voice server crashed. 1006 stands for a
		// websocket that broke off without a close code (unexpected EOF).
		return voiceSocketLost
	case 4017, 4021:
		// 4017 indicates DAVE protocol required but not supported;
		// 4021 indicates that the voice connection was dropped due to rate limiting.
		return voiceSocketRefused
	default:
		// 4006 indicates that the session is no longer valid, and Discord's
		// documentation says to start a new one. The remaining codes are
		// our bad and should never happen; they end the session all the same.
		return voiceSocketSessionOver
	}
}

// websocket opens the voice websocket for one voice server update and keeps a
// voice session on it until ctx ends or the connection fails, passing what it
// receives to the voice event handler.
//
// A session whose websocket is lost is resumed on another one. Resuming keeps
// everything the session negotiated, so its UDP socket and audio pumps carry
// on throughout. A websocket lost before its session was set up has nothing
// to resume, and the next one starts the session again.
//
// A session that the voice server ends, or will not resume, is replaced by a
// new one while voiceSessionLimit allows, provided the session had been
// working: the voice server's details were good then. Otherwise only new
// details can help, and the connection waits for the gateway to deliver them
// in a voice server update, as it does after Discord disconnects it.
func (v *VoiceConnection) websocket(ctx context.Context, endpoint string, token string) {

	v.log(LogInformational, "called")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	v.Cond.L.Lock()
	// Close a websocket if one is already open
	if v.wsCancel != nil {
		v.wsCancel()
	}
	v.wsCancel = cancel
	v.Cond.L.Unlock()

	idCtx, idCancel := context.WithTimeout(ctx, time.Second)
	err := v.waitFor(idCtx, func() bool {
		return v.Status != VoiceConnectionStatusDead && v.sessionID != ""
	})
	idCancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			v.failure(ctx, ErrVoiceNoSessionID)
		}
		return
	}

	vg := "wss://" + endpoint + "?v=8"

	// session ends with the current voice session, and endSession ends it.
	var session context.Context
	var endSession context.CancelFunc = func() {}
	sessions := 0
	// startSession begins a voice session, stopping whatever the one before
	// it had running. Nothing of that session carries over.
	startSession := func() {
		endSession()
		session, endSession = context.WithCancel(ctx)
		sessions++
		v.Cond.L.Lock()
		v.seqAck = -1
		// Discord requires each session to announce that it is speaking
		// before it sends audio. Forget the announcement made on the session
		// this one replaces (after a move or a voice server change) so the
		// sender repeats it. An announcement still on its way to that
		// session's websocket must not count for this one either.
		v.speaking = false
		v.wsConn = nil
		// The session description starts this session's pumps, exactly once.
		v.pumpsStarted = false
		v.Cond.L.Unlock()
	}
	startSession()
	// resume says the next websocket continues the session, not starts it.
	resume := false

	for attempt := 0; attempt < voiceSocketAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		socketCtx, closeSocket := context.WithCancel(session)
		defer closeSocket() // this is not needed actually, but do it to suppress lint warning

		v.Cond.L.Lock()
		v.Status = VoiceConnectionStatusConnecting
		v.Cond.Broadcast()
		v.Cond.L.Unlock()

		v.log(LogInformational, "connecting to voice endpoint %s", vg)
		// The dial must stop with its attempt: a handshake that outlives a
		// kill or a superseding voice server update would go on to identify
		// on behalf of work nobody wants any more.
		conn, err := dialVoiceWebsocket(socketCtx, v.session.Dialer, vg)
		if err == nil && socketCtx.Err() != nil {
			_ = conn.Close()
			return
		}
		if err != nil {
			if socketCtx.Err() != nil {
				return
			}
			if attempt == 0 {
				err = fmt.Errorf("error connecting to voice endpoint %s, %w", vg, err)
				v.failure(socketCtx, err)
				return
			}
			// A reconnect dial can hit a voice server that is briefly
			// unreachable (restart, failover); burn a retry slot instead of
			// declaring the connection dead on one miss.
			v.log(LogWarning, "error connecting to voice endpoint %s, retrying, %v", vg, err)
			closeSocket()
			select {
			case <-ctx.Done():
				return
			case <-time.After(voiceSocketRetryDelay):
			}
			continue
		}
		socket := voiceSocket{session: session, ctx: socketCtx, conn: conn}
		go func() {
			<-socketCtx.Done()
			if socket.session.Err() != nil {
				// The connection has finished with the session, so say so.
				// A websocket that only breaks off is what a voice server
				// keeps a session for, in case it is resumed.
				_ = conn.WriteControl(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
					time.Now().Add(voiceCloseWait),
				)
			}
			err := conn.Close()
			v.log(LogDebug, "closed voice websocket due to context done: %v", err)
		}()

		v.Cond.L.Lock()
		v.wsConn = conn
		v.Cond.L.Unlock()

		if resume {
			err = v.resumeVoiceSession(socket, token)
		} else {
			err = v.identifyVoiceSession(socket, token)
		}
		if err != nil {
			v.failure(socketCtx, err)
			return
		}

		err = v.readVoiceSocket(socket, resume)

		// Abandon the voice WS connection
		v.Cond.L.Lock()
		if v.wsConn == conn {
			v.wsConn = nil
		}
		// The session description makes a session resumable. Ready means
		// this websocket went on to serve the session: it saw the session
		// described, or resumed.
		described := v.pumpsStarted
		served := v.Status == VoiceConnectionStatusReady
		v.Cond.Broadcast()
		v.Cond.L.Unlock()

		select {
		case <-ctx.Done():
			return
		default:
		}

		end := voiceSocketEnded(err)
		switch {
		case end == voiceSocketRefused:
			err := fmt.Errorf("voice websocket closed, %w", err)
			v.failure(ctx, err)
			return

		case end == voiceSocketDisconnected:
			// We shouldn't reconnect by ourselves, but the connection is
			// not necessarily over: after a move or a voice server change
			// the gateway sends a voice server update, which starts a new
			// websocket and cancels this one. After a kick it sends a
			// voice state update, which kills the connection. If the
			// gateway sends neither (it may be down, or its own session
			// was dropped), nobody else will ever report the death.
			v.log(LogInformational, "voice websocket disconnected, awaiting a voice server update, %v", err)
			if !v.awaitVoiceServerUpdate(ctx, endSession) {
				v.failure(ctx, fmt.Errorf("%w, %w", ErrVoiceDisconnected, err))
			}
			return

		case end == voiceSocketLost && described:
			v.log(LogWarning, "voice websocket lost, resuming its session, %v", err)
			// close goroutine related to websocket
			closeSocket()
			resume = true

		case sessions < voiceSessionLimit && (described || end == voiceSocketLost):
			// The session had been working, or its websocket was lost before
			// the voice server said anything against it.
			v.log(LogWarning, "voice session ended, starting another, %v", err)
			startSession()
			resume = false

		default:
			// The voice server turned the session down from the start, or
			// has ended as many as the connection may ask it for. Only new
			// details from the gateway can help now. After a move they are
			// due: the move changes the token and puts the session beyond
			// use, and the voice server update may still be on its way.
			v.log(LogWarning, "voice session ended, awaiting a voice server update, %v", err)
			if !v.awaitVoiceServerUpdate(ctx, endSession) {
				v.failure(ctx, fmt.Errorf("voice websocket closed, %w", err))
			}
			return
		}
		if end == voiceSocketLost && !served {
			select {
			case <-ctx.Done():
				return
			case <-time.After(voiceSocketRetryDelay):
			}
		}
	}

	v.failure(ctx, ErrVoiceReconnectionLimit)
}

// identifyVoiceSession starts a voice session on socket.
func (v *VoiceConnection) identifyVoiceSession(socket voiceSocket, token string) error {
	type voiceHandshakeData struct {
		ServerID               string `json:"server_id"`
		UserID                 string `json:"user_id"`
		SessionID              string `json:"session_id"`
		Token                  string `json:"token"`
		MaxDAVEProtocolVersion int    `json:"max_dave_protocol_version"`
	}
	type voiceHandshakeOp struct {
		Op   int                `json:"op"` // Always 0
		Data voiceHandshakeData `json:"d"`
	}

	v.Cond.L.Lock()
	data := voiceHandshakeOp{0, voiceHandshakeData{
		ServerID:               v.GuildID,
		UserID:                 v.session.State.User.ID,
		SessionID:              v.sessionID,
		Token:                  token,
		MaxDAVEProtocolVersion: 1,
	}}
	v.Cond.L.Unlock()

	err := writeWebsocketJSON(socket.ctx, &v.wsMu, socket.conn, data)
	if err != nil {
		return fmt.Errorf("error sending identify packet, %w", err)
	}
	return nil
}

// resumeVoiceSession asks the voice server to carry the current voice session
// on socket. A resumed session keeps the protocol it selected, so nothing is
// negotiated again: Discord ends a session that selects its protocol a second
// time. The session's keys and DAVE state stay as they are.
func (v *VoiceConnection) resumeVoiceSession(socket voiceSocket, token string) error {
	type voiceResumeData struct {
		ServerID  string `json:"server_id"`
		SessionID string `json:"session_id"`
		Token     string `json:"token"`
		SeqAck    int    `json:"seq_ack"`
	}
	type voiceResumeOp struct {
		Op   int             `json:"op"` // Always 7
		Data voiceResumeData `json:"d"`
	}

	v.Cond.L.Lock()
	data := voiceResumeOp{7, voiceResumeData{
		ServerID:  v.GuildID,
		SessionID: v.sessionID,
		Token:     token,
		SeqAck:    v.seqAck,
	}}
	v.Cond.L.Unlock()

	v.log(LogInformational, "resuming voice websocket")
	v.log(LogDebug, "resume packet, %#v", data)

	err := writeWebsocketJSON(socket.ctx, &v.wsMu, socket.conn, data)
	if err != nil {
		return fmt.Errorf("error sending resume packet, %w", err)
	}
	return nil
}

// readVoiceSocket passes what the voice server sends on socket to the voice
// event handler until the websocket fails, and returns why it did. A
// websocket that is resuming a session fails with ErrVoiceResumeUnconfirmed
// when the voice server has not confirmed the resume within
// voiceResumeTimeout.
func (v *VoiceConnection) readVoiceSocket(socket voiceSocket, resuming bool) error {
	if resuming {
		// The event handler lifts this deadline when the resume is confirmed.
		err := socket.conn.SetReadDeadline(time.Now().Add(voiceResumeTimeout))
		if err != nil {
			return err
		}
	}
	for {
		select {
		case <-socket.ctx.Done():
			return socket.ctx.Err()
		default:
		}

		messageType, message, err := socket.conn.ReadMessage()
		if err != nil {
			var timeout net.Error
			if resuming && errors.As(err, &timeout) && timeout.Timeout() {
				return fmt.Errorf("%w, %w", ErrVoiceResumeUnconfirmed, err)
			}
			return err
		}

		// Pass received message to voice event handler
		v.onEvent(socket, messageType == websocket.BinaryMessage, message)
	}
}

// onEvent handles one message from the voice server, received on socket.
func (v *VoiceConnection) onEvent(socket voiceSocket, binary bool, message []byte) {

	if binary {
		v.log(LogDebug, "received binary: %x", message)
	} else {
		v.log(LogDebug, "received string: %s", string(message))
	}

	if binary {
		// The voice server numbers its binary messages along with the rest,
		// in their first two bytes, and a resume is sent whatever followed
		// the number it acknowledges. A binary message left out of that
		// count is sent again: a Welcome handled a second time starts the
		// frame counter over under the same key, and listeners then turn
		// away every frame whose number they have already seen.
		if len(message) >= 2 {
			v.Cond.L.Lock()
			v.seqAck = int(message[0])<<8 | int(message[1])
			v.Cond.L.Unlock()
		}
		v.handleDAVEBinary(message)
		return
	} else {

		var e voiceWebsocketMessage
		if err := json.Unmarshal(message, &e); err != nil {
			v.log(LogError, "unmarshall error, %s", err)
			return
		}

		if e.Sequence != nil {

			v.Cond.L.Lock()
			v.seqAck = *e.Sequence
			v.Cond.L.Unlock()
		}

		v.log(LogDebug, "voice operation %d", e.Operation)

		switch e.Operation {

		case 2: // READY
			op2 := voiceOP2{}

			if err := json.Unmarshal(e.RawData, &op2); err != nil {
				err := fmt.Errorf("OP2 unmarshal error, %w, %s", err, string(e.RawData))
				v.failure(socket.ctx, err)
				return
			}

			v.Cond.L.Lock()
			v.op2 = op2
			v.Cond.Broadcast()
			v.Cond.L.Unlock()

			// Start the UDP connection
			err := v.udpOpen(socket.session)
			if err != nil {
				err := fmt.Errorf("error opening udp connection, %w", err)
				v.failure(socket.ctx, err)
				return
			}

			return

		case 4: // udp encryption secret key
			op4 := voiceOP4{}
			if err := json.Unmarshal(e.RawData, &op4); err != nil {
				err := fmt.Errorf("OP4 unmarshal error, %w, %s", err, string(e.RawData))
				v.failure(socket.ctx, err)
				return
			}

			v.Cond.L.Lock()
			if socket.ctx.Err() != nil || v.Status == VoiceConnectionStatusDead {
				v.Cond.L.Unlock()
				return
			}
			v.op4 = op4
			switch op4.Mode {
			case "aead_aes256_gcm_rtpsize":
				block, err := aes.NewCipher(op4.SecretKey)
				if err != nil {
					v.Cond.L.Unlock()
					v.failure(socket.ctx, err)
					return
				}
				v.cipher, err = cipher.NewGCM(block)
				if err != nil {
					v.Cond.L.Unlock()
					v.failure(socket.ctx, err)
					return
				}
			case "aead_xchacha20_poly1305_rtpsize":
				var err error
				v.cipher, err = chacha20poly1305.NewX(op4.SecretKey)
				if err != nil {
					v.Cond.L.Unlock()
					v.failure(socket.ctx, err)
					return
				}
			default:
				err := fmt.Errorf("%w: %s", ErrVoiceUnknownEncryptionMode, op4.Mode)
				v.Cond.L.Unlock()
				v.failure(socket.ctx, err)
				return
			}

			var daveKPData []byte
			v.log(LogInformational, "DAVE protocol version %d", op4.DAVEProtocolVersion)
			if op4.DAVEProtocolVersion > 0 {
				v.dave = NewDAVESession(v.session.State.User.ID)
				for ssrc, userID := range v.ssrcToUserID {
					v.dave.SetSSRC(ssrc, userID)
				}

				var err error
				daveKPData, err = v.dave.GenerateKeyPackage()
				if err != nil {
					v.log(LogError, "DAVE key package generation failed: %s", err)
				}
			}

			// A session description can arrive more than once per session
			// (e.g. a rekey); the pumps must only be started for the first
			// one, or two senders would interleave on the same channel.
			if !v.pumpsStarted {
				v.pumpsStarted = true

				if v.OpusSend == nil {
					v.OpusSend = make(chan []byte, opusSendQueue)
				}
				go v.opusSender(socket.session, 48000, 960)

				if !v.deaf {
					if v.OpusRecv == nil {
						v.OpusRecv = make(chan *Packet, 2)
					}

					v.receivers.Add(1)
					go func() {
						defer v.receivers.Done()
						v.opusReceiver(socket.session)
					}()
				}
			}

			v.Status = VoiceConnectionStatusReady

			v.Cond.Broadcast()
			v.Cond.L.Unlock()

			if daveKPData != nil {
				v.sendDAVEKeyPackageBinary(daveKPData)
			}
			return

		case 5:
			voiceSpeakingUpdate := &VoiceSpeakingUpdate{}
			if err := json.Unmarshal(e.RawData, voiceSpeakingUpdate); err != nil {
				v.log(LogError, "OP5 unmarshall error, %s, %s", err, string(e.RawData))
				return
			}

			v.Cond.L.Lock()
			if v.ssrcToUserID == nil {
				v.ssrcToUserID = make(map[uint32]string)
			}
			v.ssrcToUserID[uint32(voiceSpeakingUpdate.SSRC)] = voiceSpeakingUpdate.UserID
			dave := v.dave
			v.Cond.L.Unlock()
			if dave != nil {
				dave.SetSSRC(uint32(voiceSpeakingUpdate.SSRC), voiceSpeakingUpdate.UserID)
			}

			for _, h := range v.voiceSpeakingUpdateHandlers {
				h(v, voiceSpeakingUpdate)
			}

		case 6: // HEARTBEAT response
			// add code to use this to track latency?
			v.log(LogDebug, "recieved heartbeat ACK")
			return

		case 12: // CLIENT CONNECT
			var op12 struct {
				UserID    string `json:"user_id"`
				AudioSSRC uint32 `json:"audio_ssrc"`
			}
			if err := json.Unmarshal(e.RawData, &op12); err != nil {
				v.log(LogError, "OP12 unmarshal error, %s, %s", err, string(e.RawData))
				return
			}
			if op12.AudioSSRC != 0 {
				v.Cond.L.Lock()
				if v.ssrcToUserID == nil {
					v.ssrcToUserID = make(map[uint32]string)
				}
				v.ssrcToUserID[op12.AudioSSRC] = op12.UserID
				dave := v.dave
				v.Cond.L.Unlock()
				if dave != nil {
					dave.SetSSRC(op12.AudioSSRC, op12.UserID)
				}
			}
			return

		case 8: // HELLO

			op8 := voiceOP8{}

			if err := json.Unmarshal(e.RawData, &op8); err != nil {
				v.log(LogError, "OP6 unmarshall error, %s, %s", err, string(e.RawData))
				return
			}
			// Start the voice websocket heartbeat to keep the connection alive
			go v.wsHeartbeat(socket.ctx, socket.conn, op8.HeartbeatInterval)

		case 9: // RESUMED
			v.log(LogInformational, "resumed voice websocket")
			// The session goes on as it was, so this websocket no longer has
			// a deadline to meet.
			if socket.conn != nil {
				_ = socket.conn.SetReadDeadline(time.Time{})
			}
			v.Cond.L.Lock()
			if socket.ctx.Err() == nil && v.Status != VoiceConnectionStatusDead && v.pumpsStarted {
				v.Status = VoiceConnectionStatusReady
				// Have the sender announce speaking on this websocket too.
				// Discord does not document whether a resumed session keeps
				// the announcement, and repeating it costs one message.
				v.speaking = false
			}
			v.Cond.Broadcast()
			v.Cond.L.Unlock()
			return

		case 13: // Client Disconnect
			v.log(LogDebug, "user disconnected: %s", string(e.RawData))
			return

		case 21: // DAVE prepare_transition
			v.handleDAVEPrepareTransition(e.RawData)
			return

		case 22: // DAVE execute_transition
			v.handleDAVEExecuteTransition(e.RawData)
			return

		case 24: // DAVE prepare_epoch
			v.handleDAVEPrepareEpoch(socket.ctx, e.RawData)
			return

		default:
			v.log(LogDebug, "unknown voice operation, %d, %s", e.Operation, string(e.RawData))
		}
	}

}

type voiceHeartbeatOp struct {
	Op   int                `json:"op"` // Always 3
	Data voiceHeartbeatData `json:"d"`
}

type voiceHeartbeatData struct {
	T      int64 `json:"t"`
	SeqAck int   `json:"seq_ack"`
}

// wsHeartbeat sends regular heartbeats to voice Discord so it knows the client
// is still connected.  If you do not send these heartbeats Discord will
// disconnect the websocket connection after a few seconds.
func (v *VoiceConnection) wsHeartbeat(ctx context.Context, wsConn *websocket.Conn, interval int) {

	if wsConn == nil {
		return
	}

	var err error
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()
	for {
		v.log(LogDebug, "sending heartbeat packet")
		v.Cond.L.Lock()
		seqAck := v.seqAck
		v.Cond.L.Unlock()
		err = writeWebsocketJSON(ctx, &v.wsMu, wsConn, voiceHeartbeatOp{3, voiceHeartbeatData{time.Now().Unix(), seqAck}})
		if err != nil {
			v.log(LogError, "error sending heartbeat to voice endpoint, %s", err)
			return
		}

		select {
		case <-ticker.C:
			// continue loop and send heartbeat
		case <-ctx.Done():
			return
		}
	}
}

// ------------------------------------------------------------------------------------------------
// Code related to the VoiceConnection UDP connection
// ------------------------------------------------------------------------------------------------

type voiceUDPData struct {
	Address string `json:"address"` // Public IP of machine running this code
	Port    uint16 `json:"port"`    // UDP Port of machine running this code
	Mode    string `json:"mode"`    // Encryption mode
}

type voiceUDPD struct {
	Protocol string       `json:"protocol"` // Always "udp" ?
	Data     voiceUDPData `json:"data"`
}

type voiceUDPOp struct {
	Op   int       `json:"op"` // Always 1
	Data voiceUDPD `json:"d"`
}

// voiceUDPDiscoveryTimeout bounds the wait for the voice server's answer to
// IP discovery, which normally arrives within one round trip.
var voiceUDPDiscoveryTimeout = 5 * time.Second

// udpOpen opens a UDP connection to the voice server and completes the
// initial required handshake.  This connection is left open in the session
// and can be used to send or receive audio.  This should only be called
// from voice.wsEvent OP2
func (v *VoiceConnection) udpOpen(ctx context.Context) (err error) {

	v.Cond.L.Lock()
	op2 := v.op2
	v.Cond.L.Unlock()

	host := op2.IP + ":" + strconv.Itoa(op2.Port)
	addr, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		v.log(LogWarning, "error resolving udp host %s, %s", host, err)
		return
	}

	v.log(LogInformational, "connecting to udp addr %s", addr.String())
	udpConn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		v.log(LogWarning, "error connecting to udp addr %s, %s", addr.String(), err)
		return
	}

	v.Cond.L.Lock()
	v.udpConn = udpConn
	v.Cond.Broadcast()
	v.Cond.L.Unlock()

	// close if context done
	go func() {
		<-ctx.Done()
		err := udpConn.Close()
		v.log(LogDebug, "closed voice UDP due to context done, %v", err)
	}()

	// Create a 74 byte array to store the packet data
	sb := make([]byte, 74)
	binary.BigEndian.PutUint16(sb, 1)            // Packet type (0x1 is request, 0x2 is response)
	binary.BigEndian.PutUint16(sb[2:], 70)       // Packet length (excluding type and length fields)
	binary.BigEndian.PutUint32(sb[4:], op2.SSRC) // The SSRC code from the Op 2 VoiceConnection event

	// And send that data over the UDP connection to Discord.
	_, err = udpConn.Write(sb)
	if err != nil {
		v.log(LogWarning, "udp write error to %s, %s", addr.String(), err)
		return
	}

	// Create a 74 byte array and listen for the initial handshake response
	// from Discord.  Once we get it parse the IP and PORT information out
	// of the response.  This should be our public IP and PORT as Discord
	// saw us.
	//
	// The request is a single datagram. If it or its response is lost, an
	// unbounded read would stall this handshake, and the websocket loop that
	// runs it, until something else cancelled the attempt.
	rb := make([]byte, 74)
	err = udpConn.SetReadDeadline(time.Now().Add(voiceUDPDiscoveryTimeout))
	if err != nil {
		return
	}
	rlen, _, err := udpConn.ReadFromUDP(rb)
	if err != nil {
		v.log(LogWarning, "udp read error, %s, %s", addr.String(), err)
		return
	}
	// The receive pump reads this socket for as long as the attempt lives.
	err = udpConn.SetReadDeadline(time.Time{})
	if err != nil {
		return
	}

	if rlen < 74 {
		v.log(LogWarning, "received udp packet too small")
		return fmt.Errorf("received udp packet too small")
	}

	// Loop over position 8 through 71 to grab the IP address. The address
	// field is null-terminated and zero-padded, so stop at the first NUL:
	// scanning on would fold the padding into the address string.
	var ip string
	for i := 8; i < len(rb)-2; i++ {
		if rb[i] == 0 {
			ip = string(rb[8:i])
			break
		}
	}

	// Grab port from position 72 and 73
	port := binary.BigEndian.Uint16(rb[len(rb)-2:])

	encryptionMode := ""
encryptionModeLoop:
	for _, mode := range op2.Modes {
		switch mode {
		case "aead_aes256_gcm_rtpsize":
			encryptionMode = mode
			break encryptionModeLoop // prefer
		case "aead_xchacha20_poly1305_rtpsize":
			encryptionMode = mode
		}
	}

	// Take the data from above and send it back to Discord to finalize
	// the UDP connection handshake.
	data := voiceUDPOp{1, voiceUDPD{"udp", voiceUDPData{ip, port, encryptionMode}}}

	v.Cond.L.Lock()
	wsConn := v.wsConn
	v.Cond.L.Unlock()
	if wsConn == nil {
		return
	}
	err = writeWebsocketJSON(ctx, &v.wsMu, wsConn, data)
	if err != nil {
		v.log(LogWarning, "udpop write error, %#v, %s", data, err)
		return
	}

	// start udpKeepAlive
	go v.udpKeepAlive(ctx, udpConn, 5*time.Second)
	// TODO: find a way to check that it fired off okay

	return
}

// udpKeepAlive sends a udp packet to keep the udp connection open
// This is still a bit of a "proof of concept"
func (v *VoiceConnection) udpKeepAlive(ctx context.Context, udpConn *net.UDPConn, i time.Duration) {
	var err error
	var sequence uint64

	packet := make([]byte, 8)

	ticker := time.NewTicker(i)
	defer ticker.Stop()
	for {

		binary.LittleEndian.PutUint64(packet, sequence)
		sequence++

		_, err = udpConn.Write(packet)
		if err != nil {
			if v.ready() {
				v.log(LogError, "write error, %s", err)
				return
			}
			v.log(LogDebug, "write error while the voice websocket is down, %s", err)
		}

		select {
		case <-ticker.C:
			// continue loop and send keepalive
		case <-ctx.Done():
			return
		}
	}
}

const (
	// opusSendQueue is how many frames may wait for the sender. Audio relayed
	// from another network arrives unevenly: a packet that was late there is
	// followed at once by the ones held up behind it. Frames dropped from
	// such a burst are heard as cuts in the audio. A full queue gives up its
	// oldest frame, so a stalled sender builds no more delay than this.
	opusSendQueue = 10

	// opusBacklogFrames is how many frames in a row may each have to wait
	// behind the one before, which is delay the listener hears, before the
	// sender sends one ahead of its turn. A queue that runs empty within a
	// second is covering for uneven arrivals. One that never does is only
	// holding the audio back, and the frame sent ahead is left to the
	// listener's own buffer to absorb.
	opusBacklogFrames = 50
)

// opusClock assigns RTP timestamps to the frames of one stream. A receiver
// measures delay by comparing each packet's timestamp with when it arrived,
// so the timestamps have to keep step with real time. Counting frames alone
// lets every pause in the audio pass for network delay, which the receiver
// then buffers against.
type opusClock struct {
	interval  time.Duration // real time covered by one frame
	samples   uint32        // timestamp units covered by one frame
	timestamp uint32        // of the next frame
	due       time.Time     // when the next frame continues the stream unbroken
}

// stamp returns the timestamp for a frame sent at now. Each whole interval
// that passed with nothing to send is left out of the stream as a gap.
func (c *opusClock) stamp(now time.Time) uint32 {
	if c.due.IsZero() {
		c.due = now
	}
	if gap := now.Sub(c.due) / c.interval; gap > 0 {
		c.timestamp += uint32(gap) * c.samples
		c.due = c.due.Add(gap * c.interval)
	}
	c.due = c.due.Add(c.interval)
	return c.stampAhead()
}

// stampAhead returns the timestamp for a frame sent ahead of its turn. It
// continues the stream without taking up any real time.
func (c *opusClock) stampAhead() uint32 {
	timestamp := c.timestamp
	c.timestamp += c.samples
	return timestamp
}

// ready reports whether the connection has a voice session and a websocket
// serving it.
//
// A session's UDP socket and pumps outlive its websockets, so they are
// running while a lost websocket is replaced. Whatever broke the websocket
// can break the UDP path too. An error there is the connection's failure
// only when the connection is ready. Otherwise the resume, or the session
// that takes this one's place, decides what becomes of the connection.
func (v *VoiceConnection) ready() bool {
	v.Cond.L.Lock()
	defer v.Cond.L.Unlock()
	return v.Status == VoiceConnectionStatusReady
}

// opusSender will listen on the given channel and send any
// pre-encoded opus audio to Discord.  Supposedly.
func (v *VoiceConnection) opusSender(ctx context.Context, rate, size int) {

	v.log(LogInformational, "called")

	v.Cond.L.Lock()
	udpConn := v.udpConn
	opusCap := 0
	if v.OpusSend != nil {
		opusCap = cap(v.OpusSend)
	}
	v.Cond.L.Unlock()
	v.log(LogInformational, "opus sender starting udp_ready=%v opus_cap=%d", udpConn != nil, opusCap)

	interval := time.Duration(size) * time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	v.sendOpus(ctx, udpConn, opusClock{interval: interval, samples: uint32(size)}, ticker.C, time.Now)
}

// sendOpus sends one queued frame for each tick until ctx ends, reading the
// time each one goes out from now. It sends a frame between ticks when
// frames have been waiting in the queue for too long.
func (v *VoiceConnection) sendOpus(
	ctx context.Context,
	udpConn *net.UDPConn,
	clock opusClock,
	ticks <-chan time.Time,
	now func() time.Time,
) {
	var sequence uint16
	var recvbuf []byte
	var ok bool
	udpHeader := make([]byte, 12)

	// The session that follows this one writes these again while it sets up.
	v.Cond.L.Lock()
	nonce := make([]byte, v.cipher.NonceSize())
	ssrc := v.op2.SSRC
	v.Cond.L.Unlock()

	// build the parts that don't change in the udpHeader
	udpHeader[0] = 0x80
	udpHeader[1] = 0x78
	binary.BigEndian.PutUint32(udpHeader[8:], ssrc)

	// start a send loop that loops until buf chan is closed
	backlog := 0
	for i := uint32(0); true; i++ {

		// A frame queued when the last one has just gone out has been
		// waiting for its turn.
		if len(v.OpusSend) > 0 {
			backlog++
		} else {
			backlog = 0
		}

		// Get data from chan.  If chan is closed, return.
		select {
		case <-ctx.Done():
			return
		case recvbuf, ok = <-v.OpusSend:
			if !ok {
				return
			}
			// else, continue loop
		}
		// The select may have taken a frame although the session has ended.
		// Going on with it would announce speaking on whichever websocket
		// the connection has by now, on behalf of a session that is gone.
		if ctx.Err() != nil {
			return
		}

		v.Cond.L.Lock()
		dave := v.dave
		daveActive := dave != nil && dave.CanEncrypt()
		speaking := v.speaking
		opusQueued := 0
		if v.OpusSend != nil {
			opusQueued = len(v.OpusSend)
		}
		v.Cond.L.Unlock()
		sampleLog := i < 5 || i%50 == 0
		if sampleLog {
			v.log(LogDebug, "opus sender dequeued idx=%d opus_len=%d queued=%d seq=%d dave_active=%v speaking=%v", i, len(recvbuf), opusQueued, sequence, daveActive, speaking)
		}

		if !speaking {
			err := v.Speaking(true)
			if err != nil {
				v.log(LogError, "error sending speaking packet idx=%d seq=%d, %s", i, sequence, err)
			}
		}

		if daveActive {
			encrypted, err := dave.EncryptFrame(recvbuf)
			if err != nil {
				v.log(LogError, "DAVE encrypt error idx=%d seq=%d opus_len=%d: %s", i, sequence, len(recvbuf), err)
			} else {
				recvbuf = encrypted
				if sampleLog {
					v.log(LogDebug, "opus sender encrypted idx=%d encrypted_len=%d seq=%d", i, len(recvbuf), sequence)
				}
			}
		}

		var timestamp uint32
		if backlog >= opusBacklogFrames {
			backlog = 0
			timestamp = clock.stampAhead()
		} else {
			// block here until we're exactly at the right time :)
			select {
			case <-ctx.Done():
				return
			case <-ticks:
				// continue
			}
			timestamp = clock.stamp(now())
		}

		// Add sequence and timestamp to udpPacket
		binary.BigEndian.PutUint16(udpHeader[2:], sequence)
		binary.BigEndian.PutUint32(udpHeader[4:], timestamp)

		binary.LittleEndian.PutUint32(nonce, i)
		v.Cond.L.Lock()
		sendbuf := make([]byte, len(udpHeader), len(udpHeader)+len(nonce)+len(recvbuf)+v.cipher.Overhead())
		copy(sendbuf, udpHeader)
		sendbuf = v.cipher.Seal(sendbuf, nonce, recvbuf, udpHeader)
		v.Cond.L.Unlock()
		sendbuf = append(sendbuf, nonce[:4]...)

		// Then send rtp audio packet to Discord over UDP
		_, err := udpConn.Write(sendbuf)

		if err != nil && v.ready() {
			v.log(LogError, "udp write failed idx=%d seq=%d timestamp=%d opus_len=%d udp_len=%d dave_active=%v: %s", i, sequence, timestamp, len(recvbuf), len(sendbuf), daveActive, err)
			err := fmt.Errorf("udp write error, %w", err)
			v.failure(ctx, err)
			return
		}
		if err != nil {
			// The frame is lost, as it would be on the way.
			v.log(LogDebug, "udp write failed while the voice websocket is down idx=%d seq=%d: %s", i, sequence, err)
		} else if sampleLog {
			v.log(LogDebug, "udp write ok idx=%d seq=%d timestamp=%d opus_len=%d udp_len=%d dave_active=%v", i, sequence, timestamp, len(recvbuf), len(sendbuf), daveActive)
		}

		// don't care if it overflows because it is already defined in Go spec
		// https://go.dev/ref/spec#Integer_overflow
		sequence++
	}
}

// A Packet contains the headers and content of a received voice packet.
type Packet struct {
	Flags       byte // first byte of RTP header
	PayloadType byte // second byte of RTP header
	Sequence    uint16
	Timestamp   uint32
	SSRC        uint32
	CSRC        []uint32
	Extension   []byte // RTP header extension with extension header, can be nil
	Opus        []byte
}

// opusReceiver listens on the UDP socket for incoming packets
// and sends them across the given channel
// NOTE :: This function may change names later.
func (v *VoiceConnection) opusReceiver(ctx context.Context) {

	v.log(LogInformational, "called")

	v.Cond.L.Lock()
	udpConn := v.udpConn
	ch := v.OpusRecv
	nonce := make([]byte, v.cipher.NonceSize())
	v.Cond.L.Unlock()

	recvbuf := make([]byte, 1024)

	for {
		rlen, err := udpConn.Read(recvbuf)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if v.ready() {
				err := fmt.Errorf("udp read error, %w", err)
				v.failure(ctx, err)
				return
			}
			// Read again, a frame's time later in case the error is one
			// that every read returns.
			v.log(LogDebug, "udp read failed while the voice websocket is down, %s", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
			// continue loop
		}

		// For now, skip anything except audio.
		if rlen < 12 || (recvbuf[0] != 0x80 && recvbuf[0] != 0x90) {
			continue
		}

		// build a audio packet struct
		p := Packet{}
		p.Flags = recvbuf[0]
		p.PayloadType = recvbuf[1]
		extentionExist := (p.Flags & 0x10) != 0 // RFC 3550 5.1
		csrcCount := (p.Flags & 0x0f)           // RFC 3550 5.1
		p.Sequence = binary.BigEndian.Uint16(recvbuf[2:4])
		p.Timestamp = binary.BigEndian.Uint32(recvbuf[4:8])
		p.SSRC = binary.BigEndian.Uint32(recvbuf[8:12])
		p.CSRC = make([]uint32, csrcCount)
		for i := range p.CSRC {
			p.CSRC[i] = binary.BigEndian.Uint32(recvbuf[12+4*i : 12+4*(i+1)])
		}
		plainLength := 12 + 4*int(csrcCount)
		if extentionExist {
			plainLength += 4
		}

		// decrypt opus data
		copy(nonce, recvbuf[rlen-4:rlen])

		v.Cond.L.Lock()
		p.Opus, err = v.cipher.Open(recvbuf[plainLength:plainLength], nonce, recvbuf[plainLength:rlen-4], recvbuf[:plainLength])
		v.Cond.L.Unlock()
		if err != nil {
			v.log(LogInformational, "failed to open udp packet, %v", err)
			continue
		}

		if extentionExist {
			extensionBegin := 12 + 4*int(csrcCount)
			extensionLength := binary.BigEndian.Uint16(recvbuf[extensionBegin+2 : extensionBegin+4])
			p.Extension = recvbuf[extensionBegin : extensionBegin+4+int(extensionLength)*4]
			p.Opus = p.Opus[int(extensionLength)*4:]
		}

		v.Cond.L.Lock()
		dave := v.dave
		v.Cond.L.Unlock()
		if dave != nil {
			decrypted, err := dave.DecryptFrame(p.SSRC, p.Opus)
			if err != nil {
				v.log(LogDebug, "DAVE decrypt error for SSRC %d: %s", p.SSRC, err)
				continue
			}
			p.Opus = decrypted
		}

		if ch != nil {
			select {
			case ch <- &p:
			case <-ctx.Done():
				return
			}
		}
	}
}

// ------------------------------------------------------------------------------------------------
// DAVE E2EE Protocol Handlers
// ------------------------------------------------------------------------------------------------

func (v *VoiceConnection) handleDAVEBinary(message []byte) {
	if len(message) < 3 {
		v.log(LogWarning, "DAVE binary message too short: %d bytes", len(message))
		return
	}

	opcode := message[2]
	payload := message[3:]
	v.log(LogDebug, "DAVE binary opcode=%d len=%d", opcode, len(payload))

	switch opcode {
	case 25:
		v.Cond.L.Lock()
		dave := v.dave
		v.Cond.L.Unlock()
		if dave != nil {
			if err := dave.HandleExternalSenderPackage(payload); err != nil {
				v.log(LogError, "DAVE external sender package failed: %s", err)
			}
		}

	case 27:
		v.log(LogDebug, "DAVE proposals (%d bytes), ignoring", len(payload))

	case 29:
		if len(payload) < 2 {
			v.log(LogWarning, "DAVE commit payload too short")
			return
		}
		transitionID := binary.BigEndian.Uint16(payload[0:2])
		v.log(LogInformational, "DAVE commit transition_id=%d, requesting re-Welcome", transitionID)

		v.Cond.L.Lock()
		dave := v.dave
		v.Cond.L.Unlock()
		if dave == nil {
			return
		}

		v.sendDAVEInvalidCommitWelcome(transitionID)

		kpData, err := dave.ResetForReWelcome()
		if err != nil {
			v.log(LogError, "DAVE reset for re-Welcome failed: %s", err)
			return
		}
		v.sendDAVEKeyPackageBinary(kpData)

		v.Cond.L.Lock()
		v.pendingReWelcome = true
		v.Cond.L.Unlock()

	case 30:
		if len(payload) < 2 {
			v.log(LogWarning, "DAVE welcome payload too short")
			return
		}
		transitionID := binary.BigEndian.Uint16(payload[0:2])
		welcomeData := payload[2:]

		v.log(LogInformational, "DAVE welcome (%d bytes) transition_id=%d", len(welcomeData), transitionID)
		v.Cond.L.Lock()
		dave := v.dave
		v.Cond.L.Unlock()
		if dave == nil {
			v.log(LogWarning, "DAVE welcome received but no session")
			return
		}

		if err := dave.HandleWelcome(welcomeData); err != nil {
			v.log(LogError, "DAVE welcome processing failed: %s", err)
			return
		}

		if err := dave.DeriveSenderKey(); err != nil {
			v.log(LogError, "DAVE sender key derivation failed: %s", err)
			return
		}

		dave.HandlePrepareTransition(transitionID, 1)
		if err := dave.ActivatePreparedTransition(transitionID); err != nil {
			v.log(LogError, "DAVE initial transition activation failed: %s", err)
			return
		}
		v.log(LogInformational, "DAVE encryption prepared after Welcome")
		v.log(LogInformational, "DAVE initial transition activated after Welcome canEncrypt=%v", dave.CanEncrypt())

		// Synchronize notification with readiness checks, after DAVE has
		// released its own lock, so a waiter cannot miss this transition.
		v.Cond.L.Lock()
		v.Cond.Broadcast()
		v.Cond.L.Unlock()

		v.sendDAVEReadyForTransition(transitionID)

		v.Cond.L.Lock()
		v.pendingReWelcome = true
		v.Cond.L.Unlock()

	default:
		v.log(LogDebug, "DAVE unknown binary opcode %d (%d bytes)", opcode, len(payload))
	}
}

func (v *VoiceConnection) handleDAVEPrepareTransition(data json.RawMessage) {
	var msg struct {
		TransitionID        uint16 `json:"transition_id"`
		DAVEProtocolVersion int    `json:"protocol_version"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		v.log(LogError, "DAVE prepare_transition unmarshal error: %s", err)
		return
	}

	v.log(LogInformational, "DAVE prepare_transition id=%d version=%d", msg.TransitionID, msg.DAVEProtocolVersion)

	v.Cond.L.Lock()
	dave := v.dave
	v.Cond.L.Unlock()
	if dave != nil {
		dave.HandlePrepareTransition(msg.TransitionID, msg.DAVEProtocolVersion)
	}
}

func (v *VoiceConnection) handleDAVEExecuteTransition(data json.RawMessage) {
	var msg struct {
		TransitionID uint16 `json:"transition_id"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		v.log(LogError, "DAVE execute_transition unmarshal error: %s", err)
		return
	}

	v.Cond.L.Lock()
	dave := v.dave
	v.Cond.L.Unlock()
	if dave != nil {
		if err := dave.HandleExecuteTransition(msg.TransitionID); err != nil {
			v.log(LogError, "DAVE execute_transition failed: %s", err)
			return
		}
		v.log(LogInformational, "DAVE execute_transition id=%d canEncrypt=%v", msg.TransitionID, dave.CanEncrypt())

		v.Cond.L.Lock()
		v.Cond.Broadcast()
		pending := v.pendingReWelcome
		v.pendingReWelcome = false
		v.Cond.L.Unlock()

		if !pending {
			v.sendDAVEReadyForTransition(msg.TransitionID)
		}
	}
}

func (v *VoiceConnection) handleDAVEPrepareEpoch(ctx context.Context, data json.RawMessage) {
	var msg struct {
		Epoch               uint64 `json:"epoch"`
		DAVEProtocolVersion int    `json:"protocol_version"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		v.log(LogError, "DAVE prepare_epoch unmarshal error: %s", err)
		return
	}

	v.log(LogInformational, "DAVE prepare_epoch epoch=%d version=%d", msg.Epoch, msg.DAVEProtocolVersion)

	v.Cond.L.Lock()
	dave := v.dave
	v.Cond.L.Unlock()
	if dave == nil {
		return
	}

	kpData, err := dave.HandlePrepareEpoch(msg.Epoch, msg.DAVEProtocolVersion)
	if err != nil {
		v.log(LogError, "DAVE prepare_epoch failed: %s", err)
		return
	}

	v.sendDAVEKeyPackageBinary(kpData)
}

func (v *VoiceConnection) RekeyDAVE() {
	v.Cond.L.Lock()
	dave := v.dave
	v.Cond.L.Unlock()
	if dave == nil {
		return
	}

	kpData, err := dave.ResetForReWelcome()
	if err != nil {
		v.log(LogError, "DAVE rekey failed: %s", err)
		return
	}
	v.sendDAVEKeyPackageBinary(kpData)
}

func (v *VoiceConnection) sendDAVEKeyPackageBinary(kpData []byte) {
	v.log(LogInformational, "DAVE sending key package (%d bytes)", len(kpData))
	binMsg := make([]byte, 1+len(kpData))
	binMsg[0] = 26
	copy(binMsg[1:], kpData)

	v.Cond.L.Lock()
	wsConn := v.wsConn
	v.Cond.L.Unlock()
	if wsConn != nil {
		err := writeWebsocketMessage(context.Background(), &v.wsMu, wsConn, websocket.BinaryMessage, binMsg)
		if err != nil {
			v.log(LogError, "DAVE key package send failed: %s", err)
		}
	}
}

func (v *VoiceConnection) sendDAVEReadyForTransition(transitionID uint16) {
	v.log(LogDebug, "DAVE sending ready_for_transition id=%d", transitionID)

	type readyData struct {
		TransitionID uint16 `json:"transition_id"`
	}
	type readyOp struct {
		Op   int       `json:"op"`
		Data readyData `json:"d"`
	}

	v.Cond.L.Lock()
	wsConn := v.wsConn
	v.Cond.L.Unlock()
	if wsConn != nil {
		err := writeWebsocketJSON(context.Background(), &v.wsMu, wsConn, readyOp{23, readyData{transitionID}})
		if err != nil {
			v.log(LogError, "DAVE ready_for_transition send failed: %s", err)
		}
	}
}

func (v *VoiceConnection) sendDAVEInvalidCommitWelcome(transitionID uint16) {
	v.log(LogInformational, "DAVE sending invalid_commit_welcome id=%d", transitionID)

	type invalidData struct {
		TransitionID uint16 `json:"transition_id"`
	}
	type invalidOp struct {
		Op   int         `json:"op"`
		Data invalidData `json:"d"`
	}

	v.Cond.L.Lock()
	wsConn := v.wsConn
	v.Cond.L.Unlock()
	if wsConn != nil {
		err := writeWebsocketJSON(context.Background(), &v.wsMu, wsConn, invalidOp{31, invalidData{transitionID}})
		if err != nil {
			v.log(LogError, "DAVE invalid_commit_welcome send failed: %s", err)
		}
	}
}
