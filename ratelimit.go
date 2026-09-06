package discordgo

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// customRateLimit holds information for defining a custom rate limit
type customRateLimit struct {
	suffix   string
	requests int
	reset    time.Duration
}

// RateLimiter holds all ratelimit buckets
type RateLimiter struct {
	sync.Mutex
	global             *int64
	buckets            map[string]*Bucket
	globalRateLimit    time.Duration
	customRateLimits   []*customRateLimit
	transientBuckets   map[string]*transientBucket
	nextTransientPrune time.Time
	now                func() time.Time
}

const transientBucketIdleTime = time.Minute

// transientBucket tracks the whole request, including response processing and
// retries when Bucket itself is unlocked. Active requests must share one bucket.
type transientBucket struct {
	bucket  *Bucket
	users   int
	expires time.Time
}

// NewRatelimiter returns a new RateLimiter
func NewRatelimiter() *RateLimiter {

	return &RateLimiter{
		buckets:          make(map[string]*Bucket),
		global:           new(int64),
		transientBuckets: make(map[string]*transientBucket),
		now:              time.Now,
		customRateLimits: []*customRateLimit{
			{
				suffix:   "//reactions//",
				requests: 1,
				reset:    200 * time.Millisecond,
			},
		},
	}
}

// GetBucket retrieves or creates a bucket
func (r *RateLimiter) GetBucket(key string) *Bucket {
	r.Lock()
	defer r.Unlock()

	if bucket, ok := r.buckets[key]; ok {
		return bucket
	}

	b := r.newBucket(key)
	r.buckets[key] = b
	return b
}

func (r *RateLimiter) newBucket(key string) *Bucket {
	b := &Bucket{
		Remaining: 1,
		Key:       key,
		global:    r.global,
	}

	// Check if there is a custom ratelimit set for this bucket ID.
	for _, rl := range r.customRateLimits {
		if strings.HasSuffix(b.Key, rl.suffix) {
			b.customRateLimit = rl
			break
		}
	}

	return b
}

// leaseTransientBucket retains short-lived endpoint state until all users finish
// and both the idle period and server's reset time have passed. Ordinary API
// buckets retain their existing lifetime. Pruning needs no background worker.
func (r *RateLimiter) leaseTransientBucket(key string) (*Bucket, func()) {
	r.Lock()
	now := r.now()
	if !now.Before(r.nextTransientPrune) {
		for key, entry := range r.transientBuckets {
			if entry.users == 0 && !now.Before(entry.expires) {
				delete(r.transientBuckets, key)
			}
		}
		r.nextTransientPrune = now.Add(transientBucketIdleTime)
	}
	entry := r.transientBuckets[key]
	if entry == nil {
		entry = &transientBucket{bucket: r.newBucket(key)}
		r.transientBuckets[key] = entry
	}
	entry.users++
	r.Unlock()

	release := func() {
		r.Lock()
		defer r.Unlock()
		entry.users--
		if entry.users != 0 {
			return
		}
		// No request can use this private bucket after its lease ends, so
		// the reset is stable while the cache lock prevents a new lease.
		entry.expires = r.now().Add(transientBucketIdleTime)
		if entry.bucket.reset.After(entry.expires) {
			entry.expires = entry.bucket.reset
		}
	}
	return entry.bucket, release
}

func (r *RateLimiter) lockTransientBucket(key string) (*Bucket, func()) {
	bucket, release := r.leaseTransientBucket(key)
	return r.LockBucketObject(bucket), release
}

// GetWaitTime returns the duration you should wait for a Bucket
func (r *RateLimiter) GetWaitTime(b *Bucket, minRemaining int) time.Duration {
	// If we ran out of calls and the reset time is still ahead of us
	// then we need to take it easy and relax a little
	if b.Remaining < minRemaining && b.reset.After(time.Now()) {
		return b.reset.Sub(time.Now())
	}

	// Check for global ratelimits
	sleepTo := time.Unix(0, atomic.LoadInt64(r.global))
	if now := time.Now(); now.Before(sleepTo) {
		return sleepTo.Sub(now)
	}

	return 0
}

// LockBucket Locks until a request can be made
func (r *RateLimiter) LockBucket(bucketID string) *Bucket {
	return r.LockBucketObject(r.GetBucket(bucketID))
}

// LockBucketObject Locks an already resolved bucket until a request can be made
func (r *RateLimiter) LockBucketObject(b *Bucket) *Bucket {
	_ = r.lockBucketObject(context.Background(), b)
	return b
}

func (r *RateLimiter) lockBucketObject(ctx context.Context, b *Bucket) error {
	if ctx.Done() == nil {
		b.Lock()
	} else {
		// Callers can hold the exported mutex directly. Polling preserves that
		// contract without leaving a goroutine waiting after cancellation.
		for !b.TryLock() {
			if err := waitForRateLimit(ctx, 10*time.Millisecond); err != nil {
				return err
			}
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			b.Unlock()
			return err
		}
		wait := r.GetWaitTime(b, 1)
		if wait <= 0 {
			break
		}
		if err := waitForRateLimit(ctx, wait); err != nil {
			b.Unlock()
			return err
		}
	}
	b.Remaining--
	return nil
}

func waitForRateLimit(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// Bucket represents a ratelimit bucket, each bucket gets ratelimited individually (-global ratelimits)
type Bucket struct {
	sync.Mutex
	Key       string
	Remaining int
	limit     int
	reset     time.Time
	global    *int64

	lastReset       time.Time
	customRateLimit *customRateLimit
	Userdata        interface{}
}

// Release unlocks the bucket and reads the headers to update the buckets ratelimit info
// and locks up the whole thing in case if there's a global ratelimit.
func (b *Bucket) Release(headers http.Header) error {
	defer b.Unlock()

	// Check if the bucket uses a custom ratelimiter
	if rl := b.customRateLimit; rl != nil {
		if time.Now().Sub(b.lastReset) >= rl.reset {
			b.Remaining = rl.requests - 1
			b.lastReset = time.Now()
		}
		if b.Remaining < 1 {
			b.reset = time.Now().Add(rl.reset)
		}
		return nil
	}

	if headers == nil {
		return nil
	}

	remaining := headers.Get("X-RateLimit-Remaining")
	reset := headers.Get("X-RateLimit-Reset")
	global := headers.Get("X-RateLimit-Global")
	resetAfter := headers.Get("X-RateLimit-Reset-After")

	// Update global and per bucket reset time if the proper headers are available
	// If global is set, then it will block all buckets until after Retry-After
	// If Retry-After without global is provided it will use that for the new reset
	// time since it's more accurate than X-RateLimit-Reset.
	// If Retry-After after is not proided, it will update the reset time from X-RateLimit-Reset
	if resetAfter != "" {
		parsedAfter, err := strconv.ParseFloat(resetAfter, 64)
		if err != nil {
			return err
		}

		whole, frac := math.Modf(parsedAfter)
		resetAt := time.Now().Add(time.Duration(whole) * time.Second).Add(time.Duration(frac*1000) * time.Millisecond)

		// Lock either this single bucket or all buckets
		if global != "" {
			atomic.StoreInt64(b.global, resetAt.UnixNano())
		} else {
			b.reset = resetAt
		}
	} else if reset != "" {
		// Calculate the reset time by using the date header returned from discord
		discordTime, err := http.ParseTime(headers.Get("Date"))
		if err != nil {
			return err
		}

		unix, err := strconv.ParseFloat(reset, 64)
		if err != nil {
			return err
		}

		// Calculate the time until reset and add it to the current local time
		// some extra time is added because without it i still encountered 429's.
		// The added amount is the lowest amount that gave no 429's
		// in 1k requests
		whole, frac := math.Modf(unix)
		delta := time.Unix(int64(whole), 0).Add(time.Duration(frac*1000)*time.Millisecond).Sub(discordTime) + time.Millisecond*250
		b.reset = time.Now().Add(delta)
	}

	// Udpate remaining if header is present
	if remaining != "" {
		parsedRemaining, err := strconv.ParseInt(remaining, 10, 32)
		if err != nil {
			return err
		}
		b.Remaining = int(parsedRemaining)
	}

	return nil
}
