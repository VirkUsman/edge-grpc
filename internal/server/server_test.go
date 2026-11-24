package server_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"github.com/usmanvirk/edge-grpc/internal/interceptors"
	"github.com/usmanvirk/edge-grpc/internal/server"
	"github.com/usmanvirk/edge-grpc/internal/testutil"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code %v (%v), want %v", got, err, want)
	}
}

func recvQuote(t *testing.T, s interface {
	Recv() (*quotesv1.Quote, error)
}) *quotesv1.Quote {
	t.Helper()
	type res struct {
		q   *quotesv1.Quote
		err error
	}
	c := make(chan res, 1)
	go func() { q, err := s.Recv(); c <- res{q, err} }()
	select {
	case r := <-c:
		if r.err != nil {
			t.Fatalf("recv: %v", r.err)
		}
		return r.q
	case <-time.After(3 * time.Second):
		t.Fatal("no quote within 3s")
	}
	return nil
}

func waitSubscribers(t *testing.T, e *testutil.Env, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for e.Hub.Subscribers() != want {
		if time.Now().After(deadline) {
			t.Fatalf("subscribers = %d, want %d", e.Hub.Subscribers(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---- unary

func TestLatestReturnsThePublishedQuote(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	e.Hub.Publish("ACME", 1234)
	q, err := e.Client.Latest(testutil.Ctx(t), &quotesv1.LatestRequest{Symbol: "ACME"})
	if err != nil || q.PriceMinor != 1234 || q.Sequence != 1 {
		t.Fatalf("%v %v", q, err)
	}
}

func TestLatestUnknownSymbolIsNotFound(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	_, err := e.Client.Latest(testutil.Ctx(t), &quotesv1.LatestRequest{Symbol: "NOPE"})
	wantCode(t, err, codes.NotFound)
}

func TestLatestEmptySymbolIsInvalidArgument(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	_, err := e.Client.Latest(testutil.Ctx(t), &quotesv1.LatestRequest{})
	wantCode(t, err, codes.InvalidArgument)
}

func TestLatestWithoutTokenIsUnauthenticated(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	_, err := e.Client.Latest(context.Background(), &quotesv1.LatestRequest{Symbol: "ACME"})
	wantCode(t, err, codes.Unauthenticated)
}

func TestLatestWithWrongTokenIsUnauthenticated(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	ctx := metadata.AppendToOutgoingContext(context.Background(), interceptors.AuthHeader, "Bearer wrong")
	_, err := e.Client.Latest(ctx, &quotesv1.LatestRequest{Symbol: "ACME"})
	wantCode(t, err, codes.Unauthenticated)
}

func TestUnaryEchoesTheCallersRequestID(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	e.Hub.Publish("ACME", 1)
	ctx := metadata.AppendToOutgoingContext(testutil.Ctx(t), interceptors.RequestIDHeader, "caller-77")
	var hdr metadata.MD
	if _, err := e.Client.Latest(ctx, &quotesv1.LatestRequest{Symbol: "ACME"}, grpc.Header(&hdr)); err != nil {
		t.Fatal(err)
	}
	if got := hdr.Get(interceptors.RequestIDHeader); len(got) != 1 || got[0] != "caller-77" {
		t.Fatalf("header %v", got)
	}
}

func TestUnaryMakesARequestIDWhenNoneSent(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	e.Hub.Publish("ACME", 1)
	var hdr metadata.MD
	if _, err := e.Client.Latest(testutil.Ctx(t), &quotesv1.LatestRequest{Symbol: "ACME"}, grpc.Header(&hdr)); err != nil {
		t.Fatal(err)
	}
	if got := hdr.Get(interceptors.RequestIDHeader); len(got) != 1 || len(got[0]) != 32 {
		t.Fatalf("header %v", got)
	}
}

func TestLatestHonoursTheDeadline(t *testing.T) {
	e := testutil.Start(t, server.Options{Delay: 2 * time.Second})
	e.Hub.Publish("ACME", 1)
	ctx, cancel := context.WithTimeout(testutil.Ctx(t), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := e.Client.Latest(ctx, &quotesv1.LatestRequest{Symbol: "ACME"})
	wantCode(t, err, codes.DeadlineExceeded)
	if time.Since(start) > time.Second {
		t.Fatalf("took %v, the deadline was 60ms", time.Since(start))
	}
}

func TestLatestCancelledByTheCaller(t *testing.T) {
	e := testutil.Start(t, server.Options{Delay: 2 * time.Second})
	e.Hub.Publish("ACME", 1)
	ctx, cancel := context.WithCancel(testutil.Ctx(t))
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_, err := e.Client.Latest(ctx, &quotesv1.LatestRequest{Symbol: "ACME"})
	wantCode(t, err, codes.Canceled)
}

func TestServerSeesTheCallersDeadlinePropagated(t *testing.T) {
	// A delayed call that the client abandons must free the server handler:
	// shutdown would otherwise wait the full two seconds for it.
	e := testutil.Start(t, server.Options{Delay: 2 * time.Second})
	e.Hub.Publish("ACME", 1)
	ctx, cancel := context.WithTimeout(testutil.Ctx(t), 40*time.Millisecond)
	defer cancel()
	e.Client.Latest(ctx, &quotesv1.LatestRequest{Symbol: "ACME"})
	sctx, scancel := context.WithTimeout(context.Background(), time.Second)
	defer scancel()
	if err := e.Server.Shutdown(sctx); err != nil {
		t.Fatalf("handler still running after its caller gave up: %v", err)
	}
}

// ---- server streaming

func TestSubscribeReceivesPublishedQuotesInOrder(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, err := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{Symbols: []string{"ACME"}})
	if err != nil {
		t.Fatal(err)
	}
	waitSubscribers(t, e, 1)
	for i := 1; i <= 5; i++ {
		e.Hub.Publish("ACME", int64(i*10))
	}
	for i := uint64(1); i <= 5; i++ {
		if q := recvQuote(t, st); q.Sequence != i || q.PriceMinor != int64(i*10) {
			t.Fatalf("got %v", q)
		}
	}
}

func TestSubscribeFiltersBySymbol(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{Symbols: []string{"BOLT"}})
	waitSubscribers(t, e, 1)
	e.Hub.Publish("ACME", 1)
	e.Hub.Publish("BOLT", 2)
	if q := recvQuote(t, st); q.Symbol != "BOLT" {
		t.Fatalf("got %v", q)
	}
}

func TestSubscribeWithNoSymbolsGetsAll(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{})
	waitSubscribers(t, e, 1)
	e.Hub.Publish("ACME", 1)
	e.Hub.Publish("BOLT", 2)
	if a, b := recvQuote(t, st), recvQuote(t, st); a.Symbol != "ACME" || b.Symbol != "BOLT" {
		t.Fatalf("%v %v", a, b)
	}
}

func TestSubscribeRejectsAnEmptySymbolInTheList(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{Symbols: []string{"ACME", ""}})
	_, err := st.Recv()
	wantCode(t, err, codes.InvalidArgument)
}

func TestSubscribeWithoutTokenIsUnauthenticated(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, err := e.Client.Subscribe(context.Background(), &quotesv1.SubscribeRequest{})
	if err == nil {
		_, err = st.Recv()
	}
	wantCode(t, err, codes.Unauthenticated)
}

func TestCancellingASubscribeFreesTheSubscription(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	ctx, cancel := context.WithCancel(testutil.Ctx(t))
	st, _ := e.Client.Subscribe(ctx, &quotesv1.SubscribeRequest{})
	waitSubscribers(t, e, 1)
	e.Hub.Publish("ACME", 1)
	recvQuote(t, st)
	cancel()
	_, err := st.Recv()
	wantCode(t, err, codes.Canceled)
	waitSubscribers(t, e, 0)
}

func TestSubscribeDeadlineEndsTheStream(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	ctx, cancel := context.WithTimeout(testutil.Ctx(t), 80*time.Millisecond)
	defer cancel()
	st, _ := e.Client.Subscribe(ctx, &quotesv1.SubscribeRequest{})
	_, err := st.Recv()
	wantCode(t, err, codes.DeadlineExceeded)
	waitSubscribers(t, e, 0)
}

func TestSubscribeAfterTheHubClosesIsUnavailable(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	e.Hub.Close()
	st, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{})
	_, err := st.Recv()
	wantCode(t, err, codes.Unavailable)
}

func TestTwoSubscribersEachGetEveryQuote(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	a, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{})
	b, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{})
	waitSubscribers(t, e, 2)
	e.Hub.Publish("ACME", 1)
	e.Hub.Publish("ACME", 2)
	for _, s := range []grpc.ServerStreamingClient[quotesv1.Quote]{a, b} {
		if x, y := recvQuote(t, s), recvQuote(t, s); x.Sequence != 1 || y.Sequence != 2 {
			t.Fatalf("%v %v", x, y)
		}
	}
}

func TestSlowReaderIsToldItWasDropped(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{})
	waitSubscribers(t, e, 1)
	// Nobody reads. The transport window fills, then the handler blocks in
	// Send, then the hub's buffer fills and the hub drops the subscriber.
	for i := 0; i < 20000; i++ {
		e.Hub.Publish("ACME", int64(i+1))
	}
	var err error
	for err == nil {
		_, err = st.Recv()
	}
	wantCode(t, err, codes.ResourceExhausted)
}

// ---- client streaming

func TestPublishCountsAcceptedAndRejected(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, err := e.Client.Publish(testutil.Ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []*quotesv1.Quote{
		{Symbol: "ACME", PriceMinor: 10},
		{Symbol: "", PriceMinor: 10},
		{Symbol: "BOLT", PriceMinor: 0},
		{Symbol: "BOLT", PriceMinor: 7},
	} {
		if err := st.Send(q); err != nil {
			t.Fatal(err)
		}
	}
	sum, err := st.CloseAndRecv()
	if err != nil || sum.Accepted != 2 || sum.Rejected != 2 {
		t.Fatalf("%v %v", sum, err)
	}
}

func TestPublishedQuotesAreVisibleToLatest(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Publish(testutil.Ctx(t))
	st.Send(&quotesv1.Quote{Symbol: "ACME", PriceMinor: 100})
	st.Send(&quotesv1.Quote{Symbol: "ACME", PriceMinor: 105})
	if _, err := st.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	q, err := e.Client.Latest(testutil.Ctx(t), &quotesv1.LatestRequest{Symbol: "ACME"})
	if err != nil || q.PriceMinor != 105 || q.Sequence != 2 {
		t.Fatalf("%v %v", q, err)
	}
}

func TestPublishReachesALiveSubscriber(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	sub, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{})
	waitSubscribers(t, e, 1)
	pub, _ := e.Client.Publish(testutil.Ctx(t))
	pub.Send(&quotesv1.Quote{Symbol: "ACME", PriceMinor: 55})
	if q := recvQuote(t, sub); q.PriceMinor != 55 {
		t.Fatalf("%v", q)
	}
	pub.CloseAndRecv()
}

func TestPublishWithNothingSentReturnsZeroes(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Publish(testutil.Ctx(t))
	sum, err := st.CloseAndRecv()
	if err != nil || sum.Accepted != 0 || sum.Rejected != 0 {
		t.Fatalf("%v %v", sum, err)
	}
}

func TestPublishWithoutTokenIsUnauthenticated(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Publish(context.Background())
	st.Send(&quotesv1.Quote{Symbol: "ACME", PriceMinor: 1})
	_, err := st.CloseAndRecv()
	wantCode(t, err, codes.Unauthenticated)
}

func TestPublishCancelledMidStream(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	ctx, cancel := context.WithCancel(testutil.Ctx(t))
	st, _ := e.Client.Publish(ctx)
	st.Send(&quotesv1.Quote{Symbol: "ACME", PriceMinor: 1})
	cancel()
	_, err := st.CloseAndRecv()
	wantCode(t, err, codes.Canceled)
}

func TestPublishAfterTheHubClosesIsUnavailable(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	e.Hub.Close()
	st, _ := e.Client.Publish(testutil.Ctx(t))
	st.Send(&quotesv1.Quote{Symbol: "ACME", PriceMinor: 1})
	_, err := st.CloseAndRecv()
	wantCode(t, err, codes.Unavailable)
}

// ---- bidirectional

func add(s string) *quotesv1.WatchRequest {
	return &quotesv1.WatchRequest{Action: &quotesv1.WatchRequest_Add{Add: s}}
}
func remove(s string) *quotesv1.WatchRequest {
	return &quotesv1.WatchRequest{Action: &quotesv1.WatchRequest_Remove{Remove: s}}
}

// settle gives the server a moment to apply requests it has read. A request
// and a published quote travel on different paths, and the test is about what
// the server does once it has seen the request.
func settle() { time.Sleep(50 * time.Millisecond) }

func TestWatchStartsEmpty(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Watch(testutil.Ctx(t))
	waitSubscribers(t, e, 1)
	e.Hub.Publish("ACME", 1)
	st.Send(add("BOLT"))
	settle()
	e.Hub.Publish("BOLT", 2)
	if q := recvQuote(t, st); q.Symbol != "BOLT" {
		t.Fatalf("got %v, ACME was never asked for", q)
	}
}

func TestWatchAddAndRemoveWhileStreaming(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Watch(testutil.Ctx(t))
	waitSubscribers(t, e, 1)
	st.Send(add("ACME"))
	settle()
	e.Hub.Publish("ACME", 1)
	if q := recvQuote(t, st); q.Symbol != "ACME" {
		t.Fatal(q)
	}
	st.Send(remove("ACME"))
	st.Send(add("BOLT"))
	settle()
	e.Hub.Publish("ACME", 2)
	e.Hub.Publish("BOLT", 3)
	if q := recvQuote(t, st); q.Symbol != "BOLT" || q.PriceMinor != 3 {
		t.Fatalf("got %v after removing ACME", q)
	}
}

func TestWatchIgnoresAnEmptyAdd(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Watch(testutil.Ctx(t))
	waitSubscribers(t, e, 1)
	st.Send(add(""))
	settle()
	e.Hub.Publish("", 1) // rejected by the hub; nothing to receive
	e.Hub.Publish("ACME", 1)
	st.Send(add("ACME"))
	settle()
	e.Hub.Publish("ACME", 2)
	if q := recvQuote(t, st); q.Sequence != 2 {
		t.Fatalf("got %v", q)
	}
}

func TestWatchEndsCleanlyWhenTheCallerCloses(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Watch(testutil.Ctx(t))
	waitSubscribers(t, e, 1)
	st.CloseSend()
	if _, err := st.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want EOF", err)
	}
	waitSubscribers(t, e, 0)
}

func TestWatchCancelFreesTheSubscription(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	ctx, cancel := context.WithCancel(testutil.Ctx(t))
	st, _ := e.Client.Watch(ctx)
	waitSubscribers(t, e, 1)
	cancel()
	_, err := st.Recv()
	wantCode(t, err, codes.Canceled)
	waitSubscribers(t, e, 0)
}

func TestWatchWithoutTokenIsUnauthenticated(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Watch(context.Background())
	_, err := st.Recv()
	wantCode(t, err, codes.Unauthenticated)
}

// ---- health

func TestHealthIsServingAndNeedsNoToken(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	h := healthpb.NewHealthClient(e.Conn)
	for _, svc := range []string{"", "quotes.v1.QuoteFeed"} {
		r, err := h.Check(context.Background(), &healthpb.HealthCheckRequest{Service: svc})
		if err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("%q: %v %v", svc, r, err)
		}
	}
}

func TestHealthUnknownServiceIsNotFound(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	_, err := healthpb.NewHealthClient(e.Conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "nope"})
	wantCode(t, err, codes.NotFound)
}

func TestHealthWatchSeesNotServingOnShutdown(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	w, err := healthpb.NewHealthClient(e.Conn).Watch(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := w.Recv(); err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("%v %v", r, err)
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		e.Server.Shutdown(ctx)
	}()
	if r, err := w.Recv(); err != nil || r.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("%v %v", r, err)
	}
}
