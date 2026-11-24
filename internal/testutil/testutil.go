// Package testutil starts a real server on an in memory listener, so tests
// exercise the actual interceptors, codec and status codes without a port.
package testutil

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"github.com/usmanvirk/edge-grpc/internal/hub"
	"github.com/usmanvirk/edge-grpc/internal/interceptors"
	"github.com/usmanvirk/edge-grpc/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// Token is accepted by every Env.
const Token = "test-token"

// Env is one running server.
type Env struct {
	Hub    *hub.Hub
	Server *server.Server
	Conn   *grpc.ClientConn
	Client quotesv1.QuoteFeedClient
	Log    *slog.Logger
	lis    *bufconn.Listener
	served chan error
}

// Start runs a server and a connection to it, both cleaned up with the test.
func Start(t testing.TB, opts server.Options) *Env {
	t.Helper()
	e := &Env{
		Hub:    hub.New(64),
		lis:    bufconn.Listen(1 << 20),
		served: make(chan error, 1),
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	e.Server = server.New(e.Hub, interceptors.NewAuthenticator(Token), e.Log, opts)
	go func() { e.served <- e.Server.Serve(e.lis) }()
	e.Conn = e.Dial(t)
	e.Client = quotesv1.NewQuoteFeedClient(e.Conn)
	t.Cleanup(func() {
		e.Conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Server.Shutdown(ctx)
		e.Hub.Close()
		<-e.served
		e.lis.Close()
	})
	return e
}

// Dialer is the dial option that routes a connection to this server.
func (e *Env) Dialer() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return e.lis.DialContext(ctx) })
}

// Dial opens another connection to the same server.
func (e *Env) Dial(t testing.TB, extra ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	opts := []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return e.lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient("passthrough:///bufnet", append(opts, extra...)...)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// Ctx returns a context carrying the valid token and a deadline.
func Ctx(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(ctx, interceptors.AuthHeader, "Bearer "+Token)
}
