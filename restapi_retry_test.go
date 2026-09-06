package discordgo

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type retryTestTransport func(*http.Request) (*http.Response, error)

func (f retryTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type retryTestBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *retryTestBody) Close() error {
	b.closed.Store(true)
	return nil
}

func newRetryTestSession(t *testing.T) *Session {
	t.Helper()
	s, err := New("Bot test")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func retryTestResponse(req *http.Request, status int, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       body,
		Request:    req,
	}
}

func awaitRetryResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("request did not finish after its deadline or cancellation")
		return nil
	}
}

func TestRESTRateLimitRetriesAreFinite(t *testing.T) {
	for _, test := range []struct {
		name     string
		retries  int
		retry429 bool
		first503 bool
		attempts int
	}{
		{name: "rate limits", retries: 2, retry429: true, attempts: 3},
		{name: "mixed failures", retries: 2, retry429: true, first503: true, attempts: 3},
		{name: "disabled rate retries", retries: 2, attempts: 1},
		{name: "zero retry budget", retry429: true, attempts: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newRetryTestSession(t)
			attempts := 0
			var previous *retryTestBody
			s.Client.Transport = retryTestTransport(func(req *http.Request) (*http.Response, error) {
				attempts++
				if previous != nil && !previous.closed.Load() {
					t.Error("retry started before the previous response body closed")
				}
				if attempts > 10 {
					return nil, errors.New("request exceeded the test's attempt limit")
				}
				previous = &retryTestBody{Reader: strings.NewReader(`{"retry_after":0}`)}
				status := http.StatusTooManyRequests
				if test.first503 && attempts == 1 {
					status = http.StatusServiceUnavailable
				}
				return retryTestResponse(req, status, previous), nil
			})
			_, err := s.Request("GET", "https://discord.test/messages", nil,
				WithRestRetries(test.retries), WithRetryOnRatelimit(test.retry429),
			)
			var rateErr *RateLimitError
			if !errors.As(err, &rateErr) {
				t.Fatalf("expected RateLimitError after retry exhaustion, got %v", err)
			}
			if attempts != test.attempts {
				t.Errorf("sent %d requests, want %d", attempts, test.attempts)
			}
			if previous == nil || !previous.closed.Load() {
				t.Error("final response body was not closed")
			}
		})
	}
}

func TestRESTRetryReplaysBodyAndSharesDeadline(t *testing.T) {
	s := newRetryTestSession(t)
	attempts := 0
	var firstDeadline time.Time
	s.Client.Transport = retryTestTransport(func(req *http.Request) (*http.Response, error) {
		attempts++
		body, err := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil || string(body) != `{"message":"test"}` {
			t.Errorf("attempt %d received body %q, error %v", attempts, body, err)
		}
		if req.Header.Get("X-Test") != "retry" {
			t.Error("request option header was lost on retry")
		}
		deadline, ok := req.Context().Deadline()
		if !ok {
			t.Error("request has no overall deadline")
		}
		if attempts == 1 {
			firstDeadline = deadline
		} else if deadline != firstDeadline {
			t.Error("retry extended the request deadline")
		}
		status := http.StatusNoContent
		if attempts == 1 {
			status = http.StatusServiceUnavailable
		} else if attempts == 2 {
			status = http.StatusTooManyRequests
		}
		return retryTestResponse(req, status, io.NopCloser(strings.NewReader(`{"retry_after":0}`))), nil
	})
	_, err := s.Request("POST", "https://discord.test/messages", map[string]string{"message": "test"}, WithHeader("X-Test", "retry"))
	if err != nil || attempts != 3 {
		t.Fatalf("request made %d attempts and returned %v", attempts, err)
	}
}

func TestRESTTimeoutIncludesRetryAfter(t *testing.T) {
	s := newRetryTestSession(t)
	s.Client.Timeout = 30 * time.Millisecond
	attempts := atomic.Int64{}
	body := &retryTestBody{Reader: strings.NewReader(`{"retry_after":0.5}`)}
	s.Client.Transport = retryTestTransport(func(req *http.Request) (*http.Response, error) {
		if attempts.Add(1) == 1 {
			return retryTestResponse(req, http.StatusTooManyRequests, body), nil
		}
		return retryTestResponse(req, http.StatusNoContent, http.NoBody), nil
	})
	done := make(chan error, 1)
	go func() { done <- s.ChannelMessageDelete("channel", "message") }()
	if err := awaitRetryResult(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected overall deadline during RetryAfter, got %v", err)
	}
	if attempts.Load() != 1 || !body.closed.Load() {
		t.Fatal("rate-limited request retried after its deadline or retained its response")
	}
}

func TestRESTCancellationInterruptsRetryAfter(t *testing.T) {
	s := newRetryTestSession(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.SyncEvents = true
	s.AddHandler(func(_ *Session, _ *RateLimit) { cancel() })
	body := &retryTestBody{Reader: strings.NewReader(`{"retry_after":2}`)}
	s.Client.Transport = retryTestTransport(func(req *http.Request) (*http.Response, error) {
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		return retryTestResponse(req, http.StatusTooManyRequests, body), nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := s.Request("GET", "https://discord.test/messages", nil, WithContext(ctx))
		done <- err
	}()
	if err := awaitRetryResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation during RetryAfter, got %v", err)
	}
	if !body.closed.Load() {
		t.Fatal("cancelled request retained its rate-limit response body")
	}
}

func TestRESTDeadlineIncludesBucketWaits(t *testing.T) {
	for _, test := range []struct {
		name       string
		held       bool
		global     bool
		callerOnly bool
	}{
		{name: "held bucket", held: true},
		{name: "bucket reset"},
		{name: "global reset", global: true},
		{name: "caller deadline", held: true, callerOnly: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newRetryTestSession(t)
			s.Client.Timeout = 30 * time.Millisecond
			endpoint := "https://discord.test/messages"
			bucket := s.Ratelimiter.GetBucket(endpoint)
			bucket.Lock()
			if test.global {
				atomic.StoreInt64(s.Ratelimiter.global, time.Now().Add(time.Hour).UnixNano())
			} else if !test.held {
				bucket.Remaining = 0
				bucket.reset = time.Now().Add(time.Hour)
			}
			if !test.held {
				bucket.Unlock()
			} else {
				defer bucket.Unlock()
			}
			requests := atomic.Int64{}
			s.Client.Transport = retryTestTransport(func(req *http.Request) (*http.Response, error) {
				requests.Add(1)
				return retryTestResponse(req, http.StatusNoContent, http.NoBody), nil
			})
			var options []RequestOption
			if test.callerOnly {
				s.Client.Timeout = time.Second
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
				defer cancel()
				options = append(options, WithContext(ctx))
			}
			done := make(chan error, 1)
			go func() {
				_, err := s.Request("GET", endpoint, nil, options...)
				done <- err
			}()
			if err := awaitRetryResult(t, done); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected bucket-wait deadline, got %v", err)
			}
			if requests.Load() != 0 {
				t.Error("request reached the transport while its bucket was unavailable")
			}
			if test.held {
				if bucket.TryLock() {
					t.Error("cancelled waiter unlocked another request's bucket")
				}
				return
			}
			if !bucket.TryLock() {
				t.Fatal("cancelled rate-limit wait retained its bucket lock")
			}
			bucket.reset = time.Time{}
			bucket.Unlock()
			atomic.StoreInt64(s.Ratelimiter.global, 0)
			if _, err := s.Request("GET", endpoint, nil); err != nil {
				t.Fatalf("bucket could not be reused after cancelled wait: %v", err)
			}
		})
	}
}

func TestRESTWaitingBucketDoesNotBlockOtherBuckets(t *testing.T) {
	s := newRetryTestSession(t)
	endpoint := "https://discord.test/blocked"
	bucket := s.Ratelimiter.LockBucket(endpoint)
	defer bucket.Release(nil)
	s.Client.Transport = retryTestTransport(func(req *http.Request) (*http.Response, error) {
		return retryTestResponse(req, http.StatusNoContent, http.NoBody), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := s.Request("GET", endpoint, nil, WithContext(ctx), func(_ *RequestConfig) { close(started) })
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("waiting request did not configure its cancellation before acquiring its bucket")
	}
	if _, err := s.Request("GET", "https://discord.test/available", nil); err != nil {
		t.Fatalf("independent bucket was blocked: %v", err)
	}
	cancel()
	if err := awaitRetryResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled waiter, got %v", err)
	}
}

func TestRESTTimeoutUsesEffectiveClient(t *testing.T) {
	for _, test := range []struct {
		name     string
		timeout  time.Duration
		want     time.Duration
		override bool
	}{
		{name: "session client", timeout: time.Second, want: time.Second},
		{name: "request client", timeout: 2 * time.Second, want: 2 * time.Second, override: true},
		{name: "client without timeout", want: 20 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newRetryTestSession(t)
			client := &http.Client{Timeout: test.timeout}
			client.Transport = retryTestTransport(func(req *http.Request) (*http.Response, error) {
				deadline, ok := req.Context().Deadline()
				remaining := time.Until(deadline)
				if !ok || remaining <= 0 || remaining > test.want {
					t.Errorf("request deadline is %v away, want at most %v", remaining, test.want)
				}
				return retryTestResponse(req, http.StatusNoContent, http.NoBody), nil
			})
			var options []RequestOption
			if test.override {
				options = append(options, WithClient(client))
			} else {
				s.Client = client
			}
			if _, err := s.Request("GET", "https://discord.test/messages", nil, options...); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRESTCancelledInteractionReleasesOnlyItsBucketLease(t *testing.T) {
	s := newRetryTestSession(t)
	interaction := &Interaction{ID: "interaction", Token: "token"}
	endpoint := EndpointInteractionResponse(interaction.ID, interaction.Token)
	bucket, release := s.Ratelimiter.lockTransientBucket(endpoint)
	defer release()
	defer bucket.Release(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.InteractionRespond(interaction, &InteractionResponse{Type: InteractionResponseDeferredChannelMessageWithSource}, WithContext(ctx))
	}()
	if err := awaitRetryResult(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected cancelled interaction waiting for bucket, got %v", err)
	}
	s.Ratelimiter.Lock()
	entry := s.Ratelimiter.transientBuckets[endpoint]
	users := entry.users
	s.Ratelimiter.Unlock()
	if users != 1 || entry.bucket != bucket {
		t.Fatalf("cancelled interaction did not preserve the active bucket's sole lease: %d users", users)
	}
}
