package discordgo

import (
	"bytes"
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
