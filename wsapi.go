// Discordgo - Discord bindings for Go
// Available at https://github.com/bwmarrin/discordgo

// Copyright 2015-2016 Bruce Marriner <bruce@sqls.net>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file contains low level functions for interacting with the Discord
// data websocket interface.

package discordgo

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ErrWSAlreadyOpen is thrown when you attempt to open
// a websocket that already is open.
var ErrWSAlreadyOpen = errors.New("web socket already opened")

// ErrWSNotFound is thrown when you attempt to use a websocket
// that doesn't exist
var ErrWSNotFound = errors.New("no websocket connection exists")

// ErrWSShardBounds is thrown when you try to use a shard ID that is
// more than the total shard count
var ErrWSShardBounds = errors.New("ShardID must be less than ShardCount")

var errReconnect = errors.New("reconnect required by gateway")

type resumePacket struct {
	Op   int `json:"op"`
	Data struct {
		Token     string `json:"token"`
		SessionID string `json:"session_id"`
		Sequence  int64  `json:"seq"`
	} `json:"d"`
}

// Open creates a websocket connection to Discord.
// See: https://discord.com/developers/docs/topics/gateway#connecting
func (s *Session) Open() error {
	return s.open(context.Background())
}

const gatewayHandshakeTimeout = 20 * time.Second

func (s *Session) open(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	if s.wsConn != nil {
		return ErrWSAlreadyOpen
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.gatewayCancel != nil {
		s.gatewayCancel()
	}
	s.gatewayContext, s.gatewayCancel = context.WithCancel(context.Background())
	return s.openLocked(ctx)
}

// openLocked is also used by reconnect, which must retain its original
// generation rather than enabling a session that the caller has closed.
func (s *Session) openLocked(ctx context.Context) (err error) {
	s.log(LogInformational, "called")
	ctx, cancel := context.WithTimeout(ctx, gatewayHandshakeTimeout)
	defer cancel()
	if s.wsConn != nil {
		return ErrWSAlreadyOpen
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	defer func() {
		var request *gatewayReconnectError
		if errors.As(err, &request) && !request.resumable {
			s.discardResumeState()
		}
	}()

	sequence := atomic.LoadInt64(s.sequence)

	var gateway string
	// Get the gateway to use for the Websocket connection
	if sequence != 0 && s.sessionID != "" && s.resumeGatewayURL != "" {
		s.log(LogDebug, "using resume gateway %s", s.resumeGatewayURL)
		gateway = s.resumeGatewayURL
	} else {
		if s.gateway == "" {
			s.gateway, err = s.Gateway(WithContext(ctx))
			if err != nil {
				return err
			}
		}

		gateway = s.gateway
	}

	// Add the version and encoding to the URL
	gateway += "?v=" + APIVersion + "&encoding=json"

	// Connect to the Gateway
	s.log(LogInformational, "connecting to gateway %s", gateway)
	header := http.Header{}
	header.Add("accept-encoding", "zlib")
	wsConn, _, err := s.Dialer.DialContext(ctx, gateway, header)
	if err != nil {
		s.log(LogError, "error connecting to gateway %s, %s", s.gateway, err)
		s.gateway = "" // clear cached gateway
		return err
	}
	s.wsMutex.Lock()
	s.wsConn = wsConn
	s.wsMutex.Unlock()

	s.wsConn.SetCloseHandler(func(code int, text string) error {
		return nil
	})

	defer func() {
		if err != nil {
			s.wsMutex.Lock()
			s.wsConn.Close()
			s.wsConn = nil
			s.wsMutex.Unlock()
		}
	}()

	// Open owns the session lock through HELLO and READY/RESUMED. Bound the
	// entire handshake so a silent gateway cannot indefinitely block Close.
	deadline, _ := ctx.Deadline()
	if err = wsConn.SetReadDeadline(deadline); err != nil {
		return err
	}
	cancelled := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		_ = wsConn.Close()
		close(cancelled)
	})
	defer func() {
		if stopCancellation != nil && !stopCancellation() {
			<-cancelled
		}
	}()

	// The first response from Discord should be an Op 10 (Hello) Packet.
	mt, m, err := s.wsConn.ReadMessage()
	if err != nil {
		return err
	}
	e, err := s.decodeGatewayEvent(mt, m)
	if err != nil {
		return err
	}
	if e.Operation != 10 {
		err = fmt.Errorf("expecting Op 10, got Op %d instead", e.Operation)
		return err
	}
	s.log(LogInformational, "Op 10 Hello Packet received from Discord")
	s.LastHeartbeatAck = time.Now().UTC()
	var h helloOp
	if err = json.Unmarshal(e.RawData, &h); err != nil {
		err = fmt.Errorf("error unmarshalling helloOp, %s", err)
		return err
	}

	// Now we send either an Op 2 Identity if this is a brand new
	// connection or Op 6 Resume if we are resuming an existing connection.
	if s.sessionID == "" && sequence == 0 {

		// Send Op 2 Identity Packet
		err = s.identify(ctx)
		if err != nil {
			err = fmt.Errorf("error sending identify packet to gateway, %s, %s", s.gateway, err)
			return err
		}

	} else {

		// Send Op 6 Resume Packet
		p := resumePacket{}
		p.Op = 6
		p.Data.Token = s.Token
		p.Data.SessionID = s.sessionID
		p.Data.Sequence = sequence

		s.log(LogInformational, "sending resume packet to gateway")
		err = s.writeGatewayJSON(ctx, p)
		if err != nil {
			err = fmt.Errorf("error sending gateway resume packet, %s, %s", s.gateway, err)
			return err
		}

	}

	// A basic state is a hard requirement for Voice.
	// We create it here so the below READY/RESUMED packet can populate
	// the state :)
	// XXX: Move to New() func?
	if s.State == nil {
		state := NewState()
		state.TrackChannels = false
		state.TrackEmojis = false
		state.TrackMembers = false
		state.TrackRoles = false
		state.TrackVoice = false
		s.State = state
	}

	// Now Discord should send us a READY or RESUMED packet.
	mt, m, err = s.wsConn.ReadMessage()
	if err != nil {
		return err
	}
	e, err = s.decodeGatewayEvent(mt, m)
	if err != nil {
		return err
	}
	if err = gatewayReconnectRequest(e); err != nil {
		return err
	}
	if e.Operation != 0 || (e.Type != `READY` && e.Type != `RESUMED`) {
		return fmt.Errorf("expecting READY/RESUMED, got Op %d Type %s", e.Operation, e.Type)
	}
	if _, err = s.onGatewayEvent(e); err != nil {
		return err
	}
	s.log(LogInformational, "First Packet:\n%#v\n", e)
	if !stopCancellation() {
		<-cancelled
	}
	stopCancellation = nil
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = wsConn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}

	s.log(LogInformational, "We are now connected to Discord, emitting connect event")
	s.handleEvent(connectEventType, &Connect{})

	// A VoiceConnections map is a hard requirement for Voice.
	// XXX: can this be moved to when opening a voice connection?
	if s.VoiceConnections == nil {
		s.log(LogInformational, "creating new VoiceConnections map")
		s.VoiceConnections = make(map[string]*VoiceConnection)
	}

	// Create listening chan outside of listen, as it needs to happen inside the
	// mutex lock and needs to exist before calling heartbeat and listen
	// go rountines.
	s.listening = make(chan interface{})

	// Start sending heartbeats and reading messages from Discord.
	go s.heartbeat(s.wsConn, s.listening, h.HeartbeatInterval)
	go s.listen(s.wsConn, s.listening)

	s.log(LogInformational, "exiting")
	return nil
}

// listen polls the websocket connection for events, it will stop when the
// listening channel is closed, or an error occurs.
func (s *Session) listen(wsConn *websocket.Conn, listening <-chan interface{}) {

	s.log(LogInformational, "called")

	for {

		messageType, message, err := wsConn.ReadMessage()

		if err != nil {

			s.reconnectConnection(wsConn, websocket.CloseNormalClosure, false)
			return
		}

		select {

		case <-listening:
			return

		default:
			_, err := s.onEvent(messageType, message)
			var request *gatewayReconnectError
			if errors.As(err, &request) {
				s.reconnectConnection(wsConn, websocket.CloseServiceRestart, !request.resumable)
				return
			}

		}
	}
}

type heartbeatOp struct {
	Op   int   `json:"op"`
	Data int64 `json:"d"`
}

type helloOp struct {
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
}

// FailedHeartbeatAcks is the Number of heartbeat intervals to wait until forcing a connection restart.
const FailedHeartbeatAcks time.Duration = 5 * time.Millisecond

// HeartbeatLatency returns the latency between heartbeat acknowledgement and heartbeat send.
func (s *Session) HeartbeatLatency() time.Duration {

	return s.LastHeartbeatAck.Sub(s.LastHeartbeatSent)

}

// heartbeat sends regular heartbeats to Discord so it knows the client
// is still connected.  If you do not send these heartbeats Discord will
// disconnect the websocket connection after a few seconds.
func (s *Session) heartbeat(wsConn *websocket.Conn, listening <-chan interface{}, heartbeatIntervalMsec time.Duration) {

	s.log(LogInformational, "called")

	if listening == nil || wsConn == nil {
		return
	}

	var err error
	ticker := time.NewTicker(heartbeatIntervalMsec * time.Millisecond)
	defer ticker.Stop()

	for {
		s.Lock()
		if s.wsConn != wsConn {
			s.Unlock()
			return
		}
		last := s.LastHeartbeatAck
		s.LastHeartbeatSent = time.Now().UTC()
		s.Unlock()
		sequence := atomic.LoadInt64(s.sequence)
		s.log(LogDebug, "sending gateway websocket heartbeat seq %d", sequence)
		err = writeWebsocketJSON(context.Background(), &s.wsMutex, wsConn, heartbeatOp{1, sequence})
		if err != nil || time.Now().UTC().Sub(last) > (heartbeatIntervalMsec*FailedHeartbeatAcks) {
			if err != nil {
				s.log(LogError, "error sending heartbeat to gateway, %s", err)
			} else {
				s.log(LogError, "haven't gotten a heartbeat ACK in %v, triggering a reconnection", time.Now().UTC().Sub(last))
			}
			s.reconnectConnection(wsConn, websocket.CloseNormalClosure, false)
			return
		}
		s.Lock()
		if s.wsConn == wsConn {
			s.DataReady = true
		}
		s.Unlock()

		select {
		case <-ticker.C:
			// continue loop and send heartbeat
		case <-listening:
			return
		}
	}
}

// UpdateStatusData is provided to UpdateStatusComplex()
type UpdateStatusData struct {
	IdleSince  *int        `json:"since"`
	Activities []*Activity `json:"activities"`
	AFK        bool        `json:"afk"`
	Status     string      `json:"status"`
}

type updateStatusOp struct {
	Op   int              `json:"op"`
	Data UpdateStatusData `json:"d"`
}

func newUpdateStatusData(idle int, activityType ActivityType, name, url string) *UpdateStatusData {
	usd := &UpdateStatusData{
		Status: "online",
	}

	if idle > 0 {
		usd.IdleSince = &idle
	}

	if name != "" {
		usd.Activities = []*Activity{{
			Name: name,
			Type: activityType,
			URL:  url,
		}}
	}

	return usd
}

// UpdateGameStatus is used to update the user's status.
// If idle>0 then set status to idle.
// If name!="" then set game.
// if otherwise, set status to active, and no activity.
func (s *Session) UpdateGameStatus(idle int, name string) (err error) {
	return s.UpdateStatusComplex(*newUpdateStatusData(idle, ActivityTypeGame, name, ""))
}

// UpdateWatchStatus is used to update the user's watch status.
// If idle>0 then set status to idle.
// If name!="" then set movie/stream.
// if otherwise, set status to active, and no activity.
func (s *Session) UpdateWatchStatus(idle int, name string) (err error) {
	return s.UpdateStatusComplex(*newUpdateStatusData(idle, ActivityTypeWatching, name, ""))
}

// UpdateStreamingStatus is used to update the user's streaming status.
// If idle>0 then set status to idle.
// If name!="" then set game.
// If name!="" and url!="" then set the status type to streaming with the URL set.
// if otherwise, set status to active, and no game.
func (s *Session) UpdateStreamingStatus(idle int, name string, url string) (err error) {
	gameType := ActivityTypeGame
	if url != "" {
		gameType = ActivityTypeStreaming
	}
	return s.UpdateStatusComplex(*newUpdateStatusData(idle, gameType, name, url))
}

// UpdateListeningStatus is used to set the user to "Listening to..."
// If name!="" then set to what user is listening to
// Else, set user to active and no activity.
func (s *Session) UpdateListeningStatus(name string) (err error) {
	return s.UpdateStatusComplex(*newUpdateStatusData(0, ActivityTypeListening, name, ""))
}

// UpdateCustomStatus is used to update the user's custom status.
// If state!="" then set the custom status.
// Else, set user to active and remove the custom status.
func (s *Session) UpdateCustomStatus(state string) (err error) {
	data := UpdateStatusData{
		Status: "online",
	}

	if state != "" {
		// Discord requires a non-empty activity name, therefore we provide "Custom Status" as a placeholder.
		data.Activities = []*Activity{{
			Name:  "Custom Status",
			Type:  ActivityTypeCustom,
			State: state,
		}}
	}

	return s.UpdateStatusComplex(data)
}

// UpdateStatusComplex allows for sending the raw status update data untouched by discordgo.
func (s *Session) UpdateStatusComplex(usd UpdateStatusData) (err error) {
	// The comment does say "untouched by discordgo", but we might need to lie a bit here.
	// The Discord documentation lists `activities` as being nullable, but in practice this
	// doesn't seem to be the case. I had filed an issue about this at
	// https://github.com/discord/discord-api-docs/issues/2559, but as of writing this
	// haven't had any movement on it, so at this point I'm assuming this is an error,
	// and am fixing this bug accordingly. Because sending `null` for `activities` instantly
	// disconnects us, I think that disallowing it from being sent in `UpdateStatusComplex`
	// isn't that big of an issue.
	if usd.Activities == nil {
		usd.Activities = make([]*Activity, 0)
	}

	s.RLock()
	defer s.RUnlock()
	if s.wsConn == nil {
		return ErrWSNotFound
	}

	err = s.writeGatewayJSON(context.Background(), updateStatusOp{3, usd})

	return
}

type requestGuildMembersData struct {
	// TODO: Deprecated. Use string instead of []string
	GuildIDs  []string  `json:"guild_id"`
	Query     *string   `json:"query,omitempty"`
	UserIDs   *[]string `json:"user_ids,omitempty"`
	Limit     int       `json:"limit"`
	Nonce     string    `json:"nonce,omitempty"`
	Presences bool      `json:"presences"`
}

type requestGuildMembersOp struct {
	Op   int                     `json:"op"`
	Data requestGuildMembersData `json:"d"`
}

// RequestGuildMembers requests guild members from the gateway
// The gateway responds with GuildMembersChunk events
// guildID   : Single Guild ID to request members of
// query     : String that username starts with, leave empty to return all members
// limit     : Max number of items to return, or 0 to request all members matched
// nonce     : Nonce to identify the Guild Members Chunk response
// presences : Whether to request presences of guild members
func (s *Session) RequestGuildMembers(guildID, query string, limit int, nonce string, presences bool) error {
	return s.RequestGuildMembersBatch([]string{guildID}, query, limit, nonce, presences)
}

// RequestGuildMembersList requests guild members from the gateway
// The gateway responds with GuildMembersChunk events
// guildID   : Single Guild ID to request members of
// userIDs   : IDs of users to fetch
// limit     : Max number of items to return, or 0 to request all members matched
// nonce     : Nonce to identify the Guild Members Chunk response
// presences : Whether to request presences of guild members
func (s *Session) RequestGuildMembersList(guildID string, userIDs []string, limit int, nonce string, presences bool) error {
	return s.RequestGuildMembersBatchList([]string{guildID}, userIDs, limit, nonce, presences)
}

// RequestGuildMembersBatch requests guild members from the gateway
// The gateway responds with GuildMembersChunk events
// guildID   : Slice of guild IDs to request members of
// query     : String that username starts with, leave empty to return all members
// limit     : Max number of items to return, or 0 to request all members matched
// nonce     : Nonce to identify the Guild Members Chunk response
// presences : Whether to request presences of guild members
//
// NOTE: this function is deprecated, please use RequestGuildMembers instead
func (s *Session) RequestGuildMembersBatch(guildIDs []string, query string, limit int, nonce string, presences bool) (err error) {
	data := requestGuildMembersData{
		GuildIDs:  guildIDs,
		Query:     &query,
		Limit:     limit,
		Nonce:     nonce,
		Presences: presences,
	}
	err = s.requestGuildMembers(data)
	return
}

// RequestGuildMembersBatchList requests guild members from the gateway
// The gateway responds with GuildMembersChunk events
// guildID   : Slice of guild IDs to request members of
// userIDs   : IDs of users to fetch
// limit     : Max number of items to return, or 0 to request all members matched
// nonce     : Nonce to identify the Guild Members Chunk response
// presences : Whether to request presences of guild members
//
// NOTE: this function is deprecated, please use RequestGuildMembersList instead
func (s *Session) RequestGuildMembersBatchList(guildIDs []string, userIDs []string, limit int, nonce string, presences bool) (err error) {
	data := requestGuildMembersData{
		GuildIDs:  guildIDs,
		UserIDs:   &userIDs,
		Limit:     limit,
		Nonce:     nonce,
		Presences: presences,
	}
	err = s.requestGuildMembers(data)
	return
}

// GatewayWriteStruct allows for sending raw gateway structs over the gateway.
func (s *Session) GatewayWriteStruct(data interface{}) (err error) {
	s.RLock()
	defer s.RUnlock()
	if s.wsConn == nil {
		return ErrWSNotFound
	}

	err = s.writeGatewayJSON(context.Background(), data)

	return err
}

func (s *Session) requestGuildMembers(data requestGuildMembersData) (err error) {
	s.log(LogInformational, "called")

	s.RLock()
	defer s.RUnlock()
	if s.wsConn == nil {
		return ErrWSNotFound
	}

	err = s.writeGatewayJSON(context.Background(), requestGuildMembersOp{8, data})

	return
}

// onEvent is the "event handler" for all messages received on the
// Discord Gateway API websocket connection.
//
// If you use the AddHandler() function to register a handler for a
// specific event this function will pass the event along to that handler.
//
// If you use the AddHandler() function to register a handler for the
// "OnEvent" event then all events will be passed to that handler.
func (s *Session) onEvent(messageType int, message []byte) (*Event, error) {
	e, err := s.decodeGatewayEvent(messageType, message)
	if err != nil {
		return e, err
	}
	return s.onGatewayEvent(e)
}

func (s *Session) decodeGatewayEvent(messageType int, message []byte) (*Event, error) {

	var err error
	var reader io.Reader
	reader = bytes.NewBuffer(message)

	// If this is a compressed message, uncompress it.
	if messageType == websocket.BinaryMessage {

		z, err2 := zlib.NewReader(reader)
		if err2 != nil {
			s.log(LogError, "error uncompressing websocket message, %s", err)
			return nil, err2
		}

		defer func() {
			err3 := z.Close()
			if err3 != nil {
				s.log(LogWarning, "error closing zlib, %s", err)
			}
		}()

		reader = z
	}

	// Decode the event into an Event struct.
	var e *Event
	decoder := json.NewDecoder(reader)
	if err = decoder.Decode(&e); err != nil {
		s.log(LogError, "error decoding websocket message, %s", err)
		return e, err
	}
	if e == nil {
		return nil, errors.New("gateway sent a null event")
	}
	return e, nil
}

type gatewayReconnectError struct {
	resumable bool
}

func (e *gatewayReconnectError) Error() string {
	return fmt.Sprintf("gateway requested reconnect (resumable: %t)", e.resumable)
}

func (e *gatewayReconnectError) Unwrap() error {
	return errReconnect
}

func gatewayReconnectRequest(e *Event) error {
	switch e.Operation {
	case 7:
		return &gatewayReconnectError{resumable: true}
	case 9:
		var resumable bool
		if err := json.Unmarshal(e.RawData, &resumable); err != nil {
			return err
		}
		return &gatewayReconnectError{resumable: resumable}
	default:
		return nil
	}
}

func (s *Session) onGatewayEvent(e *Event) (*Event, error) {
	if err := gatewayReconnectRequest(e); err != nil {
		// The caller owns the originating socket and decides whether the
		// request is still current. Open also calls this while holding Lock.
		return e, err
	}
	var err error

	s.log(LogDebug, "Op: %d, Seq: %d, Type: %s, Data: %s\n\n", e.Operation, e.Sequence, e.Type, string(e.RawData))

	// Ping request.
	// Must respond with a heartbeat packet within 5 seconds
	if e.Operation == 1 {
		s.log(LogInformational, "sending heartbeat in response to Op1")
		err = s.writeGatewayJSON(context.Background(), heartbeatOp{1, atomic.LoadInt64(s.sequence)})
		if err != nil {
			s.log(LogError, "error sending heartbeat in response to Op1")
			return e, err
		}

		return e, nil
	}

	if e.Operation == 10 {
		// Op10 is handled by Open()
		return e, nil
	}

	if e.Operation == 11 {
		s.Lock()
		s.LastHeartbeatAck = time.Now().UTC()
		s.Unlock()
		s.log(LogDebug, "got heartbeat ACK")
		return e, nil
	}

	// Do not try to Dispatch a non-Dispatch Message
	if e.Operation != 0 {
		// But we probably should be doing something with them.
		// TEMP
		s.log(LogWarning, "unknown Op: %d, Seq: %d, Type: %s, Data: %s", e.Operation, e.Sequence, e.Type, string(e.RawData))
		return e, nil
	}

	// Store the message sequence
	atomic.StoreInt64(s.sequence, e.Sequence)

	// Map event to registered event handlers and pass it along to any registered handlers.
	if eh, ok := registeredInterfaceProviders[e.Type]; ok {
		e.Struct = eh.New()

		// Attempt to unmarshal our event.
		if err = json.Unmarshal(e.RawData, e.Struct); err != nil {
			s.log(LogError, "error unmarshalling %s event, %s", e.Type, err)
		}

		// Send event to any registered event handlers for it's type.
		// Because the above doesn't cancel this, in case of an error
		// the struct could be partially populated or at default values.
		// However, most errors are due to a single field and I feel
		// it's better to pass along what we received than nothing at all.
		// TODO: Think about that decision :)
		// Either way, READY events must fire, even with errors.
		s.handleEvent(e.Type, e.Struct)
	} else {
		s.log(LogWarning, "unknown event: Op: %d, Seq: %d, Type: %s, Data: %s", e.Operation, e.Sequence, e.Type, string(e.RawData))
	}

	// For legacy reasons, we send the raw event also, this could be useful for handling unknown events.
	s.handleEvent(eventEventType, e)

	return e, nil
}

// ------------------------------------------------------------------------------------------------
// Code related to voice connections that initiate over the data websocket
// ------------------------------------------------------------------------------------------------

type voiceChannelJoinData struct {
	GuildID   *string `json:"guild_id"`
	ChannelID *string `json:"channel_id"`
	SelfMute  bool    `json:"self_mute"`
	SelfDeaf  bool    `json:"self_deaf"`
}

type voiceChannelJoinOp struct {
	Op   int                  `json:"op"`
	Data voiceChannelJoinData `json:"d"`
}

// ChannelVoiceJoin joins the session user to a voice channel.
//
//	gID     : Guild ID of the channel to join.
//	cID     : Channel ID of the channel to join.
//	mute    : If true, you will be set to muted upon joining.
//	deaf    : If true, you will be set to deafened upon joining.
func (s *Session) ChannelVoiceJoin(ctx context.Context, gID, cID string, mute, deaf bool) (voice *VoiceConnection, err error) {

	s.log(LogInformational, "called")

	s.RLock()
	voice = s.VoiceConnections[gID]
	s.RUnlock()

	if voice == nil {
		voice = &VoiceConnection{}
		s.Lock()
		s.VoiceConnections[gID] = voice
		s.Unlock()
	}

	voice.Cond = sync.NewCond(&sync.Mutex{})
	voice.Cond.L.Lock()
	voice.Status = VoiceConnectionStatusNew
	voice.dead = make(chan struct{})
	voice.Dead = voice.dead
	voice.GuildID = gID
	voice.session = s
	voice.LogLevel = s.LogLevel
	voice.Cond.L.Unlock()

	err = s.voiceStateUpdate(ctx, gID, cID, mute, deaf)
	if err != nil {
		return
	}

	err = voice.waitUntilStatus(ctx, VoiceConnectionStatusReady)

	return
}

// VoiceStateUpdate initiates a voice session to a voice channel, but does not complete it.
//
// This should only be used when the VoiceServerUpdate will be intercepted and used elsewhere.
//
//	gID     : Guild ID of the channel to join.
//	cID     : Channel ID of the channel to join, leave empty to disconnect.
//	mute    : If true, you will be set to muted upon joining.
//	deaf    : If true, you will be set to deafened upon joining.
func (s *Session) VoiceStateUpdate(gID, cID string, mute, deaf bool) (err error) {
	return s.voiceStateUpdate(context.Background(), gID, cID, mute, deaf)
}

// voiceStateUpdate includes the gateway write in the caller's join/leave
// deadline. It must remain usable while Open or Close holds the session lock.
func (s *Session) voiceStateUpdate(
	ctx context.Context,
	gID, cID string,
	mute, deaf bool,
) (err error) {

	s.log(LogInformational, "called")

	var channelID *string
	if cID == "" {
		channelID = nil
	} else {
		channelID = &cID
	}

	// Send the request to Discord that we want to join the voice channel
	data := voiceChannelJoinOp{4, voiceChannelJoinData{&gID, channelID, mute, deaf}}
	err = s.writeGatewayJSON(ctx, data)
	return
}

// onVoiceStateUpdate handles Voice State Update events on the data websocket.
func (s *Session) onVoiceStateUpdate(st *VoiceStateUpdate) {

	// Check if we have a voice connection to update
	s.RLock()
	voice, exists := s.VoiceConnections[st.GuildID]
	s.RUnlock()
	if !exists {
		return
	}

	// We only care about events that are about us.
	if s.State.User.ID != st.UserID {
		return
	}

	// Store the SessionID for later use.
	if st.ChannelID == "" {
		voice.Kill()
	} else {
		voice.Cond.L.Lock()
		defer voice.Cond.L.Unlock()
		voice.sessionID = st.SessionID
		voice.mute = st.Mute
		voice.deaf = st.Deaf
		voice.Cond.Broadcast()
	}
}

// onVoiceServerUpdate handles the Voice Server Update data websocket event.
//
// This is also fired if the Guild's voice region changes while connected
// to a voice channel.  In that case, need to re-establish connection to
// the new region endpoint.
func (s *Session) onVoiceServerUpdate(ev *VoiceServerUpdate) {

	s.log(LogInformational, "called")

	s.RLock()
	voice, exists := s.VoiceConnections[ev.GuildID]
	s.RUnlock()

	// If no VoiceConnection exists, just skip this
	if !exists {
		return
	}

	// Open a connection to the voice server
	err := voice.onVoiceServerUpdate(ev)
	if err != nil {
		s.log(LogError, "onVoiceServerUpdate voice.open, %s", err)
	}
}

type identifyOp struct {
	Op   int      `json:"op"`
	Data Identify `json:"d"`
}

// identify sends the identify packet to the gateway
func (s *Session) identify(ctx context.Context) error {
	s.log(LogDebug, "called")

	// TODO: This is a temporary block of code to help
	// maintain backwards compatibility
	if s.Compress == false {
		s.Identify.Compress = false
	}

	// TODO: This is a temporary block of code to help
	// maintain backwards compatibility
	if s.Token != "" && s.Identify.Token == "" {
		s.Identify.Token = s.Token
	}

	// TODO: Below block should be refactored so ShardID and ShardCount
	// can be deprecated and their usage moved to the Session.Identify
	// struct
	if s.ShardCount > 1 {

		if s.ShardID >= s.ShardCount {
			return ErrWSShardBounds
		}

		s.Identify.Shard = &[2]int{s.ShardID, s.ShardCount}
	}

	// Send Identify packet to Discord
	op := identifyOp{2, s.Identify}
	s.log(LogDebug, "Identify Packet: \n%#v", op)
	err := s.writeGatewayJSON(ctx, op)

	return err
}

const maxResumeAttempts = 3

func (s *Session) discardResumeState() {
	s.resumeGatewayURL = ""
	s.sessionID = ""
	atomic.StoreInt64(s.sequence, 0)
}

func (s *Session) reconnectConnection(connection *websocket.Conn, closeCode int, invalidate bool) {
	s.Lock()
	// A late read/heartbeat failure must never close a replacement connection.
	if connection == nil || s.wsConn != connection {
		s.Unlock()
		return
	}
	ctx := s.gatewayContext
	err := s.closeLocked(closeCode)
	if invalidate {
		s.discardResumeState()
	}
	s.Unlock()
	if err != nil {
		s.log(LogWarning, "error closing session connection, %s", err)
	}
	s.handleEvent(disconnectEventType, &Disconnect{})
	s.reconnect(ctx)
}

func (s *Session) reconnect(ctx context.Context) {
	if ctx == nil {
		return
	}
	wait := time.Second
	failures := 0
	for {
		s.Lock()
		if ctx.Err() != nil || s.gatewayContext != ctx || !s.ShouldReconnectOnError {
			s.Unlock()
			return
		}
		err := s.openLocked(ctx)
		if err != nil && !errors.Is(err, ErrWSAlreadyOpen) {
			failures++
			if failures >= maxResumeAttempts && s.sessionID != "" {
				s.log(LogWarning, "discarding resume information after %d failed reconnects, next attempt will identify", failures)
				s.discardResumeState()
			}
		}
		s.Unlock()
		if err == nil || errors.Is(err, ErrWSAlreadyOpen) {
			// Voice sockets survive gateway interruptions independently. They
			// transition to Dead if Discord invalidates their voice session.
			return
		}
		s.log(LogError, "error reconnecting to gateway, %s", err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		wait *= 2
		if wait > 10*time.Minute {
			wait = 10 * time.Minute
		}
	}
}

// Close closes a websocket and stops listening, heartbeat and reconnect work.
// TODO: Add support for Voice WS/UDP
func (s *Session) Close() error {
	return s.CloseWithCode(websocket.CloseNormalClosure)
}

// CloseWithCode closes a websocket using the provided closeCode and stops all
// listening, heartbeat and reconnect work.
// TODO: Add support for Voice WS/UDP connections
func (s *Session) CloseWithCode(closeCode int) (err error) {

	s.log(LogInformational, "called")
	s.Lock()
	if s.gatewayCancel != nil {
		s.gatewayCancel()
	}
	err = s.closeLocked(closeCode)
	s.Unlock()
	s.handleEvent(disconnectEventType, &Disconnect{})
	return err
}

func (s *Session) closeLocked(closeCode int) (err error) {
	s.DataReady = false

	if s.listening != nil {
		s.log(LogInformational, "closing listening channel")
		close(s.listening)
		s.listening = nil
	}

	// TODO: Close all active Voice Connections too
	// this should force stop any reconnecting voice channels too

	if s.wsConn != nil {

		s.log(LogInformational, "sending close frame")
		// To cleanly close a connection, a client should send a close
		// frame and wait for the server to close the connection.
		err := writeWebsocketMessage(
			context.Background(), &s.wsMutex, s.wsConn,
			websocket.CloseMessage, websocket.FormatCloseMessage(closeCode, ""),
		)
		if err != nil {
			s.log(LogInformational, "error closing websocket, %s", err)
		}

		// TODO: Wait for Discord to actually close the connection.
		time.Sleep(1 * time.Second)

		s.log(LogInformational, "closing gateway websocket")
		err = s.wsConn.Close()
		if err != nil {
			s.log(LogInformational, "error closing websocket, %s", err)
		}

		s.wsMutex.Lock()
		s.wsConn = nil
		s.wsMutex.Unlock()
	}

	return
}
