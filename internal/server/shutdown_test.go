package server_test

import (
	"context"
	"testing"
	"time"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"github.com/usmanvirk/edge-grpc/internal/server"
	"github.com/usmanvirk/edge-grpc/internal/testutil"
	"google.golang.org/grpc/codes"
)

func shutdownWithin(t *testing.T, e *testutil.Env, d time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return e.Server.Shutdown(ctx)
}

func TestShutdownWithNothingOpenIsImmediate(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	start := time.Now()
	if err := shutdownWithin(t, e, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("took %v", time.Since(start))
	}
}

func TestShutdownDrainsAnOpenSubscribeStream(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{})
	waitSubscribers(t, e, 1)
	for i := 1; i <= 3; i++ {
		e.Hub.Publish("ACME", int64(i))
	}
	if err := shutdownWithin(t, e, 2*time.Second); err != nil {
		t.Fatalf("shutdown gave up on an open stream: %v", err)
	}
	// Everything published before shutdown still arrives, then the stream
	// ends with Unavailable so a client knows to reconnect elsewhere.
	for want := uint64(1); want <= 3; want++ {
		if q := recvQuote(t, st); q.Sequence != want {
			t.Fatalf("got #%d, want #%d", q.Sequence, want)
		}
	}
	_, err := st.Recv()
	wantCode(t, err, codes.Unavailable)
}

func TestShutdownDrainsAnOpenWatchStream(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Watch(testutil.Ctx(t))
	waitSubscribers(t, e, 1)
	st.Send(add("ACME"))
	settle()
	e.Hub.Publish("ACME", 1)
	e.Hub.Publish("BOLT", 2) // not watched, must not be flushed
	if err := shutdownWithin(t, e, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if q := recvQuote(t, st); q.Symbol != "ACME" {
		t.Fatalf("got %v", q)
	}
	_, err := st.Recv()
	wantCode(t, err, codes.Unavailable)
}

func TestShutdownLetsAnInFlightUnaryCallFinish(t *testing.T) {
	e := testutil.Start(t, server.Options{Delay: 300 * time.Millisecond})
	e.Hub.Publish("ACME", 9)
	result := make(chan error, 1)
	go func() {
		_, err := e.Client.Latest(testutil.Ctx(t), &quotesv1.LatestRequest{Symbol: "ACME"})
		result <- err
	}()
	time.Sleep(80 * time.Millisecond) // let the call reach the handler
	if err := shutdownWithin(t, e, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("in flight call was cut: %v", err)
	}
}

func TestShutdownTwiceIsHarmless(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	if err := shutdownWithin(t, e, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := shutdownWithin(t, e, time.Second); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestNewCallsAfterShutdownFail(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	e.Hub.Publish("ACME", 1)
	if err := shutdownWithin(t, e, time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(testutil.Ctx(t), time.Second)
	defer cancel()
	_, err := e.Client.Latest(ctx, &quotesv1.LatestRequest{Symbol: "ACME"})
	wantCode(t, err, codes.Unavailable)
}

func TestShutdownGivesUpOnAPublishStreamThatNeverEnds(t *testing.T) {
	// A client holding a Publish stream open is not told to leave, so the
	// deadline is what ends it. Shutdown reports that it had to stop hard.
	e := testutil.Start(t, server.Options{})
	st, _ := e.Client.Publish(testutil.Ctx(t))
	st.Send(&quotesv1.Quote{Symbol: "ACME", PriceMinor: 1})
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	err := shutdownWithin(t, e, 200*time.Millisecond)
	if err != context.DeadlineExceeded {
		t.Fatalf("got %v, want the context error", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %v", time.Since(start))
	}
	_, rerr := st.CloseAndRecv()
	wantCode(t, rerr, codes.Unavailable)
}

func TestShutdownDrainsManyStreamsAtOnce(t *testing.T) {
	e := testutil.Start(t, server.Options{})
	var streams []interface {
		Recv() (*quotesv1.Quote, error)
	}
	for i := 0; i < 25; i++ {
		st, _ := e.Client.Subscribe(testutil.Ctx(t), &quotesv1.SubscribeRequest{})
		streams = append(streams, st)
	}
	waitSubscribers(t, e, 25)
	e.Hub.Publish("ACME", 1)
	if err := shutdownWithin(t, e, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	for i, st := range streams {
		if q := recvQuote(t, st); q.Sequence != 1 {
			t.Fatalf("stream %d: %v", i, q)
		}
		_, err := st.Recv()
		wantCode(t, err, codes.Unavailable)
	}
}
