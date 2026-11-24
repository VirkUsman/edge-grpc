package client

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryPolicy is exponential backoff with jitter. The zero value does not
// retry at all.
type RetryPolicy struct {
	// MaxAttempts counts the first try. 1 or less means no retry.
	MaxAttempts int
	// Initial is the wait after the first failure.
	Initial time.Duration
	// Max caps a single wait.
	Max time.Duration
	// Multiplier grows the wait each attempt. 2 if unset.
	Multiplier float64
	// Jitter, 0 to 1, shortens each wait by a random fraction of up to that
	// much. With 0.5 a 100ms wait lands between 50ms and 100ms. It stops a
	// crowd of clients that failed together from retrying together.
	Jitter float64
	// Retryable decides which codes are worth another try. Unavailable only
	// if unset: it is the one code that means the call did not run.
	Retryable func(codes.Code) bool
}

// DefaultRetry is four tries, so three waits: about 50, 100 and 200ms before
// jitter.
var DefaultRetry = RetryPolicy{
	MaxAttempts: 4,
	Initial:     50 * time.Millisecond,
	Max:         time.Second,
	Multiplier:  2,
	Jitter:      0.4,
}

func (p RetryPolicy) retryable(c codes.Code) bool {
	if p.Retryable != nil {
		return p.Retryable(c)
	}
	return c == codes.Unavailable
}

// Backoff is the wait after the given failed attempt (1 is the first
// failure). rnd returns a value in [0,1); pass rand.Float64 outside tests.
func (p RetryPolicy) Backoff(attempt int, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	m := p.Multiplier
	if m < 1 {
		m = 2
	}
	d := float64(p.Initial)
	for i := 1; i < attempt; i++ {
		d *= m
		if p.Max > 0 && d >= float64(p.Max) {
			d = float64(p.Max)
			break
		}
	}
	if p.Max > 0 && d > float64(p.Max) {
		d = float64(p.Max)
	}
	j := p.Jitter
	if j < 0 {
		j = 0
	}
	if j > 1 {
		j = 1
	}
	return time.Duration(d * (1 - j*rnd()))
}

// sleep waits d or until ctx ends. It reports whether the wait completed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// UnaryRetry retries a unary call on the policy's codes. It gives up early
// when the caller's deadline is closer than the next wait, because sleeping
// into a certain DeadlineExceeded helps nobody.
func UnaryRetry(p RetryPolicy) grpc.UnaryClientInterceptor {
	return unaryRetry(p, rand.Float64)
}

func unaryRetry(p RetryPolicy, rnd func() float64) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var err error
		for attempt := 1; ; attempt++ {
			err = invoker(ctx, method, req, reply, cc, opts...)
			if err == nil || attempt >= p.MaxAttempts || !p.retryable(status.Code(err)) {
				return err
			}
			wait := p.Backoff(attempt, rnd)
			if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= wait {
				return err
			}
			if !sleep(ctx, wait) {
				return status.FromContextError(ctx.Err()).Err()
			}
		}
	}
}
