package client_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"github.com/usmanvirk/edge-grpc/internal/client"
	"github.com/usmanvirk/edge-grpc/internal/hub"
	"github.com/usmanvirk/edge-grpc/internal/interceptors"
	"github.com/usmanvirk/edge-grpc/internal/server"
	"github.com/usmanvirk/edge-grpc/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func dial(t *testing.T, e *testutil.Env, cfg client.Config) *client.Client {
	t.Helper()
	cfg.Plaintext = true
	c, err := client.Dial("passthrough:///bufnet", cfg, e.Dialer())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestLatestSendsTheToken(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	e.Hub.Publish("ACME", 5)
	c := dial(t, e, client.Config{Token: testutil.Token})
	q, err := c.Latest(ctx(t), "ACME")
	if err != nil || q.PriceMinor != 5 {
		t.Fatalf("%v %v", q, err)
	}
}

func TestLatestWithTheWrongTokenIsNotRetried(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	c := dial(t, e, client.Config{Token: "wrong", Retry: client.DefaultRetry})
	start := time.Now()
	_, err := c.Latest(ctx(t), "ACME")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code %v", status.Code(err))
	}
	if time.Since(start) > 40*time.Millisecond {
		t.Fatalf("took %v, looks retried", time.Since(start))
	}
}

func TestLatestNotFoundPassesThrough(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	c := dial(t, e, client.Config{Token: testutil.Token})
	_, err := c.Latest(ctx(t), "NOPE")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code %v", status.Code(err))
	}
}

// recorder is a QuoteFeed that fails Latest a set number of times and records
// the request ids it saw.
type recorder struct {
	quotesv1.UnimplementedQuoteFeedServer
	mu       sync.Mutex
	failures int
	ids      []string
}

func (r *recorder) Latest(ctx context.Context, req *quotesv1.LatestRequest) (*quotesv1.Quote, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, md.Get(interceptors.RequestIDHeader)...)
	if r.failures > 0 {
		r.failures--
		return nil, status.Error(codes.Unavailable, "warming up")
	}
	return &quotesv1.Quote{Symbol: req.Symbol, PriceMinor: 1, Sequence: 1}, nil
}

func startRecorder(t *testing.T, r *recorder) grpc.DialOption {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	quotesv1.RegisterQuoteFeedServer(gs, r)
	go gs.Serve(lis)
	t.Cleanup(func() { gs.Stop() })
	return grpc.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) { return lis.DialContext(c) })
}

func TestRetryReachesARealServerThatRecovers(t *testing.T) {
	r := &recorder{failures: 2}
	c, _ := client.Dial("passthrough:///x", client.Config{Plaintext: true, Retry: client.RetryPolicy{MaxAttempts: 5, Initial: time.Millisecond}}, startRecorder(t, r))
	defer c.Close()
	if _, err := c.Latest(ctx(t), "ACME"); err != nil {
		t.Fatal(err)
	}
	if len(r.ids) != 3 {
		t.Fatalf("server saw %d calls, want 3", len(r.ids))
	}
}

func TestARetriedCallKeepsOneRequestID(t *testing.T) {
	r := &recorder{failures: 2}
	c, _ := client.Dial("passthrough:///x", client.Config{Plaintext: true, Retry: client.RetryPolicy{MaxAttempts: 5, Initial: time.Millisecond}}, startRecorder(t, r))
	defer c.Close()
	c.Latest(ctx(t), "ACME")
	if len(r.ids) != 3 || r.ids[0] == "" || r.ids[0] != r.ids[1] || r.ids[1] != r.ids[2] {
		t.Fatalf("ids across attempts: %v", r.ids)
	}
}

func TestSeparateCallsGetSeparateRequestIDs(t *testing.T) {
	r := &recorder{}
	c, _ := client.Dial("passthrough:///x", client.Config{Plaintext: true}, startRecorder(t, r))
	defer c.Close()
	c.Latest(ctx(t), "A")
	c.Latest(ctx(t), "A")
	if len(r.ids) != 2 || r.ids[0] == r.ids[1] {
		t.Fatalf("ids: %v", r.ids)
	}
}

func TestRetryGivesUpAndReturnsUnavailable(t *testing.T) {
	r := &recorder{failures: 100}
	c, _ := client.Dial("passthrough:///x", client.Config{Plaintext: true, Retry: client.RetryPolicy{MaxAttempts: 3, Initial: time.Millisecond}}, startRecorder(t, r))
	defer c.Close()
	_, err := c.Latest(ctx(t), "ACME")
	if status.Code(err) != codes.Unavailable || len(r.ids) != 3 {
		t.Fatalf("code %v after %d calls", status.Code(err), len(r.ids))
	}
}

func TestPublishReturnsTheSummary(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	c := dial(t, e, client.Config{Token: testutil.Token})
	sum, err := c.Publish(ctx(t), []*quotesv1.Quote{
		{Symbol: "ACME", PriceMinor: 1}, {Symbol: "ACME", PriceMinor: 2}, {Symbol: "", PriceMinor: 3},
	})
	if err != nil || sum.Accepted != 2 || sum.Rejected != 1 {
		t.Fatalf("%v %v", sum, err)
	}
}

func TestPublishWithTheWrongTokenReportsUnauthenticated(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	c := dial(t, e, client.Config{Token: "wrong"})
	big := make([]*quotesv1.Quote, 50)
	for i := range big {
		big[i] = &quotesv1.Quote{Symbol: "ACME", PriceMinor: 1}
	}
	_, err := c.Publish(ctx(t), big)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code %v (%v)", status.Code(err), err)
	}
}

func TestFollowDeliversQuotesUntilTheContextEnds(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	c := dial(t, e, client.Config{Token: testutil.Token})
	cx, cancel := context.WithCancel(ctx(t))
	got := make(chan *quotesv1.Quote, 8)
	done := make(chan error, 1)
	go func() {
		done <- c.Follow(cx, []string{"ACME"}, func(q *quotesv1.Quote) error { got <- q; return nil })
	}()
	for e.Hub.Subscribers() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	e.Hub.Publish("ACME", 1)
	e.Hub.Publish("ACME", 2)
	for want := uint64(1); want <= 2; want++ {
		select {
		case q := <-got:
			if q.Sequence != want {
				t.Fatalf("got #%d, want #%d", q.Sequence, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no quote")
		}
	}
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("Follow returned %v", err)
	}
}

func TestFollowStopsWithTheCallbacksError(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	c := dial(t, e, client.Config{Token: testutil.Token, Retry: client.DefaultRetry})
	stop := io.ErrClosedPipe
	done := make(chan error, 1)
	go func() {
		done <- c.Follow(ctx(t), nil, func(*quotesv1.Quote) error { return stop })
	}()
	for e.Hub.Subscribers() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	e.Hub.Publish("ACME", 1)
	if err := <-done; err != stop {
		t.Fatalf("got %v, want the callback's error", err)
	}
}

func TestFollowDoesNotRetryAnAuthFailure(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	c := dial(t, e, client.Config{Token: "wrong", Retry: client.DefaultRetry})
	err := c.Follow(ctx(t), nil, func(*quotesv1.Quote) error { return nil })
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code %v", status.Code(err))
	}
}

// swap is a dialer that can be pointed at a different listener, which is how a
// client sees one server replaced by another at the same address.
type swap struct {
	cur atomic.Pointer[bufconn.Listener]
}

func (s *swap) option() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return s.cur.Load().DialContext(ctx) })
}

func startOn(t *testing.T, h *hub.Hub) (*server.Server, *bufconn.Listener) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := server.New(h, interceptors.NewAuthenticator(testutil.Token), slog.New(slog.NewTextHandler(io.Discard, nil)), server.Options{})
	go s.Serve(lis)
	return s, lis
}

func TestFollowReconnectsToAReplacementServer(t *testing.T) {
	h := hub.New(64)
	t.Cleanup(h.Close)
	s1, l1 := startOn(t, h)
	var sw swap
	sw.cur.Store(l1)
	var gaps atomic.Int32
	c, _ := client.Dial("passthrough:///x", client.Config{
		Token: testutil.Token, Plaintext: true,
		Retry: client.RetryPolicy{MaxAttempts: 20, Initial: 5 * time.Millisecond, Max: 20 * time.Millisecond},
		OnGap: func(string, uint64) { gaps.Add(1) },
	}, sw.option())
	defer c.Close()

	got := make(chan uint64, 16)
	done := make(chan error, 1)
	cx, cancel := context.WithCancel(ctx(t))
	go func() {
		done <- c.Follow(cx, []string{"ACME"}, func(q *quotesv1.Quote) error { got <- q.Sequence; return nil })
	}()
	for h.Subscribers() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	h.Publish("ACME", 1)
	if seq := <-got; seq != 1 {
		t.Fatalf("first quote #%d", seq)
	}

	// The first server drains. Follow sees Unavailable, backs off, and the
	// second server answers at the same address.
	sc, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	if err := s1.Shutdown(sc); err != nil {
		t.Fatal(err)
	}
	s2, l2 := startOn(t, h)
	defer s2.Shutdown(context.Background())
	sw.cur.Store(l2)
	deadline := time.Now().Add(3 * time.Second)
	for h.Subscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("never resubscribed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.Publish("ACME", 2)
	select {
	case seq := <-got:
		if seq != 2 {
			t.Fatalf("after reconnect got #%d", seq)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("nothing after reconnect")
	}
	if gaps.Load() != 0 {
		t.Fatalf("reported %d gaps, none were missed", gaps.Load())
	}
	cancel()
	<-done
}

func TestFollowReportsAGapWhenQuotesWereMissed(t *testing.T) {
	h := hub.New(64)
	t.Cleanup(h.Close)
	s1, l1 := startOn(t, h)
	var sw swap
	sw.cur.Store(l1)
	missed := make(chan uint64, 4)
	c, _ := client.Dial("passthrough:///x", client.Config{
		Token: testutil.Token, Plaintext: true,
		Retry: client.RetryPolicy{MaxAttempts: 50, Initial: 5 * time.Millisecond, Max: 10 * time.Millisecond},
		OnGap: func(_ string, n uint64) { missed <- n },
	}, sw.option())
	defer c.Close()
	got := make(chan uint64, 16)
	cx, cancel := context.WithCancel(ctx(t))
	done := make(chan error, 1)
	go func() {
		done <- c.Follow(cx, nil, func(q *quotesv1.Quote) error { got <- q.Sequence; return nil })
	}()
	for h.Subscribers() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	h.Publish("ACME", 1)
	<-got
	sc, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	s1.Shutdown(sc)
	h.Publish("ACME", 2) // nobody is listening
	h.Publish("ACME", 3) // nobody is listening
	s2, l2 := startOn(t, h)
	defer s2.Shutdown(context.Background())
	sw.cur.Store(l2)
	for h.Subscribers() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	h.Publish("ACME", 4)
	if seq := <-got; seq != 4 {
		t.Fatalf("got #%d", seq)
	}
	select {
	case n := <-missed:
		if n != 2 {
			t.Fatalf("reported %d missed, want 2", n)
		}
	case <-time.After(time.Second):
		t.Fatal("gap not reported")
	}
	cancel()
	<-done
}

func TestFollowGivesUpWhenTheServerNeverComesBack(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	c := dial(t, e, client.Config{Token: testutil.Token, Retry: client.RetryPolicy{MaxAttempts: 3, Initial: time.Millisecond, Max: 2 * time.Millisecond}})
	sc, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	e.Server.Shutdown(sc)
	err := c.Follow(ctx(t), nil, func(*quotesv1.Quote) error { return nil })
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v", err)
	}
}
