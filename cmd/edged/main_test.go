package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"github.com/usmanvirk/edge-grpc/internal/client"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestValidate(t *testing.T) {
	ok := config{tokens: []string{"t"}, symbols: []string{"A"}, tick: time.Second}
	if err := ok.validate(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*config){
		"no token":     func(c *config) { c.tokens = []string{"", ""} },
		"empty symbol": func(c *config) { c.symbols = []string{""} },
		"zero tick":    func(c *config) { c.tick = 0 },
	}
	for name, mut := range cases {
		c := ok
		mut(&c)
		if c.validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestSecondTokenAlsoValidates(t *testing.T) {
	c := config{tokens: []string{"", "next"}, symbols: []string{"A"}, tick: time.Second}
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
}

// startReal runs the whole daemon on a real localhost TCP port.
func startReal(t *testing.T, drain time.Duration) (addr string, stop func() error) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	cfg := config{tokens: []string{"real-token"}, symbols: []string{"ACME", "BOLT"}, tick: 10 * time.Millisecond, drain: drain}
	go func() { done <- run(ctx, cfg, lis, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	stopped := false
	stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("daemon did not stop")
		}
		return nil
	}
	t.Cleanup(func() { stop() })
	return lis.Addr().String(), stop
}

func dial(t *testing.T, addr, token string) *client.Client {
	t.Helper()
	c, err := client.Dial(addr, client.Config{Token: token, Plaintext: true, Retry: client.DefaultRetry})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestServesLatestOverARealSocket(t *testing.T) {
	addr, _ := startReal(t, time.Second)
	c := dial(t, addr, "real-token")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var q *quotesv1.Quote
	var err error
	for i := 0; i < 50; i++ { // the feed publishes its first prices just after Serve starts
		if q, err = c.Latest(ctx, "ACME"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || q.PriceMinor <= 0 {
		t.Fatalf("%v %v", q, err)
	}
}

func TestRealSocketRejectsABadToken(t *testing.T) {
	addr, _ := startReal(t, time.Second)
	c := dial(t, addr, "nope")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Latest(ctx, "ACME"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code %v", status.Code(err))
	}
}

func TestFollowSeesTheFeedTick(t *testing.T) {
	addr, _ := startReal(t, time.Second)
	c := dial(t, addr, "real-token")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n := 0
	err := c.Follow(ctx, []string{"BOLT"}, func(q *quotesv1.Quote) error {
		n++
		if n == 5 {
			cancel()
		}
		return nil
	})
	if status.Code(err) != codes.Canceled || n != 5 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestSignalDrainsAnOpenStreamAndExitsCleanly(t *testing.T) {
	addr, stop := startReal(t, 3*time.Second)
	c := dial(t, addr, "real-token")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := make(chan struct{}, 1)
	followErr := make(chan error, 1)
	go func() {
		followErr <- c.Follow(ctx, nil, func(*quotesv1.Quote) error {
			select {
			case got <- struct{}{}:
			default:
			}
			return nil
		})
	}()
	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("no quotes before stop")
	}
	start := time.Now()
	if err := stop(); err != nil {
		t.Fatalf("run returned %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("drain took %v with a stream open", time.Since(start))
	}
	cancel()
	<-followErr
}

func TestRunReturnsWhenTheListenerDies(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	lis.Close()
	cfg := config{tokens: []string{"t"}, symbols: []string{"A"}, tick: time.Hour, drain: time.Second}
	err := run(context.Background(), cfg, lis, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("expected the serve error")
	}
}
