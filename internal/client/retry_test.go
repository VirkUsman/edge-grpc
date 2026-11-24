package client

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func zero() float64 { return 0 }

func TestBackoffDoublesThenCaps(t *testing.T) {
	p := RetryPolicy{Initial: 10 * time.Millisecond, Max: 70 * time.Millisecond, Multiplier: 2}
	want := []time.Duration{10, 20, 40, 70, 70, 70}
	for i, w := range want {
		if got := p.Backoff(i+1, zero); got != w*time.Millisecond {
			t.Errorf("attempt %d: %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
}

func TestBackoffMultiplierDefaultsToTwo(t *testing.T) {
	p := RetryPolicy{Initial: 5 * time.Millisecond}
	if got := p.Backoff(3, zero); got != 20*time.Millisecond {
		t.Fatalf("got %v", got)
	}
}

func TestBackoffJitterStaysInsideItsBounds(t *testing.T) {
	p := RetryPolicy{Initial: 100 * time.Millisecond, Multiplier: 2, Jitter: 0.5}
	if got := p.Backoff(1, zero); got != 100*time.Millisecond {
		t.Errorf("rnd=0: %v", got)
	}
	if got := p.Backoff(1, func() float64 { return 0.999999 }); got < 50*time.Millisecond || got > 51*time.Millisecond {
		t.Errorf("rnd~1: %v, want about 50ms", got)
	}
}

func TestBackoffJitterIsClamped(t *testing.T) {
	hi := RetryPolicy{Initial: 100 * time.Millisecond, Jitter: 7}
	if got := hi.Backoff(1, func() float64 { return 0.5 }); got != 50*time.Millisecond {
		t.Errorf("jitter above 1: %v", got)
	}
	lo := RetryPolicy{Initial: 100 * time.Millisecond, Jitter: -1}
	if got := lo.Backoff(1, func() float64 { return 0.5 }); got != 100*time.Millisecond {
		t.Errorf("negative jitter: %v", got)
	}
}

func TestBackoffNeverExceedsMaxEvenWithJitterOff(t *testing.T) {
	p := RetryPolicy{Initial: time.Second, Max: 3 * time.Second, Multiplier: 10}
	for a := 1; a < 40; a++ {
		if got := p.Backoff(a, zero); got > 3*time.Second {
			t.Fatalf("attempt %d: %v", a, got)
		}
	}
}

func TestBackoffTreatsAttemptZeroAsOne(t *testing.T) {
	p := RetryPolicy{Initial: 8 * time.Millisecond}
	if p.Backoff(0, zero) != p.Backoff(1, zero) {
		t.Fatal("attempt 0 differs from 1")
	}
}

func TestDefaultRetryOnlyRetriesUnavailable(t *testing.T) {
	p := RetryPolicy{}
	for c := codes.Code(0); c <= codes.Unauthenticated; c++ {
		if got, want := p.retryable(c), c == codes.Unavailable; got != want {
			t.Errorf("%v: retryable %v", c, got)
		}
	}
}

func TestCustomRetryableOverridesTheDefault(t *testing.T) {
	p := RetryPolicy{Retryable: func(c codes.Code) bool { return c == codes.ResourceExhausted }}
	if !p.retryable(codes.ResourceExhausted) || p.retryable(codes.Unavailable) {
		t.Fatal("custom rule ignored")
	}
}

// flaky fails the first n calls with code, then succeeds.
func flaky(n int, code codes.Code, calls *int) grpc.UnaryInvoker {
	return func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		*calls++
		if *calls <= n {
			return status.Error(code, "nope")
		}
		return nil
	}
}

func fast(attempts int) RetryPolicy {
	return RetryPolicy{MaxAttempts: attempts, Initial: time.Millisecond, Max: 2 * time.Millisecond}
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	var calls int
	err := unaryRetry(fast(5), zero)(context.Background(), "/m", nil, nil, nil, flaky(3, codes.Unavailable, &calls))
	if err != nil || calls != 4 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
}

func TestRetryStopsAtMaxAttempts(t *testing.T) {
	var calls int
	err := unaryRetry(fast(3), zero)(context.Background(), "/m", nil, nil, nil, flaky(100, codes.Unavailable, &calls))
	if status.Code(err) != codes.Unavailable || calls != 3 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
}

func TestRetryDoesNotRetryOtherCodes(t *testing.T) {
	for _, c := range []codes.Code{codes.NotFound, codes.InvalidArgument, codes.Unauthenticated, codes.Internal, codes.DeadlineExceeded} {
		var calls int
		err := unaryRetry(fast(5), zero)(context.Background(), "/m", nil, nil, nil, flaky(100, c, &calls))
		if status.Code(err) != c || calls != 1 {
			t.Errorf("%v: err %v after %d calls", c, err, calls)
		}
	}
}

func TestZeroPolicyMakesOneCall(t *testing.T) {
	var calls int
	unaryRetry(RetryPolicy{}, zero)(context.Background(), "/m", nil, nil, nil, flaky(100, codes.Unavailable, &calls))
	if calls != 1 {
		t.Fatalf("%d calls", calls)
	}
}

func TestRetryStopsWhenTheContextIsCancelledDuringTheWait(t *testing.T) {
	var calls int
	ctx, cancel := context.WithCancel(context.Background())
	p := RetryPolicy{MaxAttempts: 10, Initial: time.Hour}
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	start := time.Now()
	err := unaryRetry(p, zero)(ctx, "/m", nil, nil, nil, flaky(100, codes.Unavailable, &calls))
	if status.Code(err) != codes.Canceled || calls != 1 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
	if time.Since(start) > time.Second {
		t.Fatal("waited out the backoff")
	}
}

func TestRetryDoesNotSleepPastTheDeadline(t *testing.T) {
	var calls int
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	p := RetryPolicy{MaxAttempts: 10, Initial: time.Hour}
	start := time.Now()
	err := unaryRetry(p, zero)(ctx, "/m", nil, nil, nil, flaky(100, codes.Unavailable, &calls))
	if status.Code(err) != codes.Unavailable || calls != 1 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
	if time.Since(start) > 40*time.Millisecond {
		t.Fatalf("slept %v into a deadline it could not meet", time.Since(start))
	}
}

func TestRetryWaitsGrowBetweenAttempts(t *testing.T) {
	var stamps []time.Time
	inv := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		stamps = append(stamps, time.Now())
		return status.Error(codes.Unavailable, "x")
	}
	p := RetryPolicy{MaxAttempts: 4, Initial: 20 * time.Millisecond, Multiplier: 2}
	unaryRetry(p, zero)(context.Background(), "/m", nil, nil, nil, inv)
	if len(stamps) != 4 {
		t.Fatalf("%d attempts", len(stamps))
	}
	g1, g2, g3 := stamps[1].Sub(stamps[0]), stamps[2].Sub(stamps[1]), stamps[3].Sub(stamps[2])
	if g1 < 18*time.Millisecond || g2 < 38*time.Millisecond || g3 < 78*time.Millisecond {
		t.Fatalf("gaps %v %v %v, want about 20 40 80ms", g1, g2, g3)
	}
}

func TestSleepReportsWhetherItCompleted(t *testing.T) {
	if !sleep(context.Background(), time.Millisecond) {
		t.Error("short sleep reported cancelled")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleep(ctx, time.Hour) {
		t.Error("sleep on a cancelled context reported complete")
	}
	if sleep(ctx, 0) {
		t.Error("zero sleep on a cancelled context reported complete")
	}
}

func TestRetryKeepsTheLastErrorsMessage(t *testing.T) {
	n := 0
	inv := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		n++
		return status.Errorf(codes.Unavailable, "attempt %d", n)
	}
	err := unaryRetry(fast(3), zero)(context.Background(), "/m", nil, nil, nil, inv)
	if got := status.Convert(err).Message(); got != "attempt 3" {
		t.Fatalf("message %q", got)
	}
}
