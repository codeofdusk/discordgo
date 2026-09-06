package discordgo

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type interactionTestTransport func(*http.Request) (*http.Response, error)

func (f interactionTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testRateLimitClock(r *RateLimiter) *atomic.Int64 {
	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixNano())
	r.now = func() time.Time { return time.Unix(0, clock.Load()) }
	return clock
}

func TestInteractionBucketCacheStaysBounded(t *testing.T) {
	s, err := New("Bot test")
	if err != nil {
		t.Fatal(err)
	}
	clock := testRateLimitClock(s.Ratelimiter)
	s.Client.Transport = interactionTestTransport(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})
	for i := 0; i < 10000; i++ {
		interaction := &Interaction{ID: fmt.Sprint(i), Token: fmt.Sprintf("token-%d", i)}
		resp := &InteractionResponse{Type: InteractionResponseDeferredChannelMessageWithSource}
		if err := s.InteractionRespond(interaction, resp); err != nil {
			t.Fatal(err)
		}
		clock.Add(int64(time.Second))
		if count := len(s.Ratelimiter.transientBuckets); count > 120 {
			t.Fatalf("cached %d interaction buckets after %d commands", count, i+1)
		}
	}
	if count := len(s.Ratelimiter.buckets); count != 0 {
		t.Fatalf("interaction callbacks retained %d ordinary buckets", count)
	}
}

func TestInteractionBucketSurvivesActiveRequestAndReset(t *testing.T) {
	r := NewRatelimiter()
	clock := testRateLimitClock(r)
	bucket, release := r.lockTransientBucket("interaction")
	if err := bucket.Release(http.Header{"X-Ratelimit-Remaining": {"10"}}); err != nil {
		t.Fatal(err)
	}
	clock.Add(int64(2 * transientBucketIdleTime))
	other, releaseOther := r.lockTransientBucket("other")
	_ = other.Release(nil)
	releaseOther()
	shared, releaseShared := r.lockTransientBucket("interaction")
	if shared != bucket {
		t.Fatal("active request lost its bucket during pruning")
	}
	// A server reset can outlive the idle timeout; it must not be discarded.
	reset := r.now().Add(10 * transientBucketIdleTime)
	shared.reset = reset
	_ = shared.Release(nil)
	releaseShared()
	release()
	clock.Add(int64(2 * transientBucketIdleTime))
	other, releaseOther = r.lockTransientBucket("other")
	_ = other.Release(nil)
	releaseOther()
	if r.transientBuckets["interaction"].bucket != bucket {
		t.Fatal("bucket was pruned before its server reset")
	}
	clock.Store(reset.Add(time.Second).UnixNano())
	other, releaseOther = r.lockTransientBucket("other")
	_ = other.Release(nil)
	releaseOther()
	if r.transientBuckets["interaction"] != nil {
		t.Fatal("idle bucket survived past its server reset")
	}
}

func TestInteractionRetriesShareBucket(t *testing.T) {
	s, err := New("Bot test")
	if err != nil {
		t.Fatal(err)
	}
	clock := testRateLimitClock(s.Ratelimiter)
	interaction := &Interaction{ID: "interaction", Token: "token"}
	endpoint := EndpointInteractionResponse(interaction.ID, interaction.Token)
	requests := 0
	var first *Bucket
	s.Client.Transport = interactionTestTransport(func(req *http.Request) (*http.Response, error) {
		requests++
		entry := s.Ratelimiter.transientBuckets[endpoint]
		if entry == nil || entry.users != 1 {
			t.Fatal("retry did not retain the interaction bucket")
		}
		if first == nil {
			first = entry.bucket
		} else if first != entry.bucket {
			t.Fatal("retry changed buckets")
		}
		status := http.StatusNoContent
		body := ""
		if requests == 1 {
			status = http.StatusTooManyRequests
			body = `{"retry_after":0}`
			clock.Add(int64(2 * transientBucketIdleTime))
			other, release := s.Ratelimiter.lockTransientBucket("other")
			_ = other.Release(nil)
			release()
		}
		return &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})
	resp := &InteractionResponse{Type: InteractionResponseDeferredChannelMessageWithSource}
	if err := s.InteractionRespond(interaction, resp); err != nil {
		t.Fatal(err)
	}
	if requests != 2 || s.Ratelimiter.transientBuckets[endpoint].users != 0 {
		t.Fatal("retry did not finish and release its cache lease")
	}
}

func TestInteractionWaitingRequestsRetainBucket(t *testing.T) {
	r := NewRatelimiter()
	clock := testRateLimitClock(r)
	bucket, release := r.lockTransientBucket("interaction")
	done := make(chan *Bucket, 1)
	go func() {
		shared, releaseShared := r.lockTransientBucket("interaction")
		_ = shared.Release(nil)
		releaseShared()
		done <- shared
	}()
	deadline := time.Now().Add(time.Second)
	for {
		r.Lock()
		users := r.transientBuckets["interaction"].users
		r.Unlock()
		if users == 2 {
			break
		}
		if time.Now().After(deadline) {
			_ = bucket.Release(nil)
			release()
			t.Fatal("second request did not acquire its cache lease")
		}
		time.Sleep(time.Millisecond)
	}
	clock.Add(int64(2 * transientBucketIdleTime))
	other, releaseOther := r.lockTransientBucket("other")
	_ = other.Release(nil)
	releaseOther()
	_ = bucket.Release(nil)
	release()
	select {
	case shared := <-done:
		if shared != bucket {
			t.Fatal("waiting request lost its shared bucket")
		}
	case <-time.After(time.Second):
		t.Fatal("waiting request did not complete")
	}
}
