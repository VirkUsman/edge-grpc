package interceptors_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"github.com/usmanvirk/edge-grpc/internal/interceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// lockedBuffer is a log sink that two goroutines may write to.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type panicky struct {
	quotesv1.UnimplementedQuoteFeedServer
}

func (panicky) Latest(context.Context, *quotesv1.LatestRequest) (*quotesv1.Quote, error) {
	panic("latest blew up")
}

func (panicky) Subscribe(*quotesv1.SubscribeRequest, grpc.ServerStreamingServer[quotesv1.Quote]) error {
	panic("subscribe blew up")
}

func start(t *testing.T, log *slog.Logger) quotesv1.QuoteFeedClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(interceptors.Chain(log, interceptors.NewAuthenticator("tok"))...)
	quotesv1.RegisterQuoteFeedServer(gs, panicky{})
	go gs.Serve(lis)
	conn, err := grpc.NewClient("passthrough:///b",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); gs.Stop() })
	return quotesv1.NewQuoteFeedClient(conn)
}

func authed() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), interceptors.AuthHeader, "Bearer tok")
}

func TestChainSurvivesAPanicInAUnaryHandler(t *testing.T) {
	var logs lockedBuffer
	c := start(t, slog.New(slog.NewTextHandler(&logs, nil)))
	_, err := c.Latest(authed(), &quotesv1.LatestRequest{Symbol: "A"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code %v", status.Code(err))
	}
	// The process is still up and serving: a second call is answered too.
	if _, err := c.Latest(authed(), &quotesv1.LatestRequest{Symbol: "A"}); status.Code(err) != codes.Internal {
		t.Fatalf("second call: %v", err)
	}
	if !strings.Contains(logs.String(), "latest blew up") {
		t.Fatalf("panic not logged: %s", logs.String())
	}
}

func TestChainSurvivesAPanicInAStreamHandler(t *testing.T) {
	c := start(t, slog.New(slog.NewTextHandler(&lockedBuffer{}, nil)))
	st, err := c.Subscribe(authed(), &quotesv1.SubscribeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); status.Code(err) != codes.Internal {
		t.Fatalf("code %v", status.Code(err))
	}
}

func TestChainRejectsBeforeTheHandlerCanPanic(t *testing.T) {
	c := start(t, slog.New(slog.NewTextHandler(&lockedBuffer{}, nil)))
	_, err := c.Latest(context.Background(), &quotesv1.LatestRequest{Symbol: "A"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code %v, want Unauthenticated not Internal", status.Code(err))
	}
}

func TestChainLogsTheRequestIDOfARejectedCall(t *testing.T) {
	var logs lockedBuffer
	c := start(t, slog.New(slog.NewTextHandler(&logs, nil)))
	ctx := metadata.AppendToOutgoingContext(context.Background(), interceptors.RequestIDHeader, "trace-me")
	c.Latest(ctx, &quotesv1.LatestRequest{Symbol: "A"})
	if !strings.Contains(logs.String(), "request_id=trace-me") || !strings.Contains(logs.String(), "code=Unauthenticated") {
		t.Fatalf("log: %s", logs.String())
	}
}
