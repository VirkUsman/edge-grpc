// Package client wraps the generated QuoteFeed client with what a caller
// needs to be safe by default: the token on every call, a request id that
// survives retries, retry with backoff, and a Follow that reconnects.
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math/rand/v2"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"github.com/usmanvirk/edge-grpc/internal/interceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Config for Dial.
type Config struct {
	Token string
	// Plaintext skips TLS. For local runs and tests.
	Plaintext bool
	Retry     RetryPolicy
	// OnGap is called by Follow when sequence numbers skip, which means quotes
	// were missed while it was disconnected.
	OnGap func(symbol string, missed uint64)
}

// Client talks to one server.
type Client struct {
	conn *grpc.ClientConn
	feed quotesv1.QuoteFeedClient
	cfg  Config
	rnd  func() float64
}

// Dial connects lazily; the first call is what opens the connection.
func Dial(target string, cfg Config, extra ...grpc.DialOption) (*Client, error) {
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	if cfg.Plaintext {
		creds = insecure.NewCredentials()
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		// The headers interceptor is outermost so a retried call keeps the
		// same request id, which is the point of having one.
		grpc.WithChainUnaryInterceptor(unaryHeaders(cfg.Token), UnaryRetry(cfg.Retry)),
		grpc.WithChainStreamInterceptor(streamHeaders(cfg.Token)),
	}
	conn, err := grpc.NewClient(target, append(opts, extra...)...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, feed: quotesv1.NewQuoteFeedClient(conn), cfg: cfg, rnd: rand.Float64}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func withHeaders(ctx context.Context, token string) context.Context {
	if token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, interceptors.AuthHeader, "Bearer "+token)
	}
	if md, ok := metadata.FromOutgoingContext(ctx); !ok || len(md.Get(interceptors.RequestIDHeader)) == 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, interceptors.RequestIDHeader, interceptors.NewRequestID())
	}
	return ctx
}

func unaryHeaders(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return inv(withHeaders(ctx, token), method, req, reply, cc, opts...)
	}
}

func streamHeaders(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(withHeaders(ctx, token), desc, cc, method, opts...)
	}
}

// Latest fetches the most recent quote for a symbol, retrying per the policy.
func (c *Client) Latest(ctx context.Context, symbol string) (*quotesv1.Quote, error) {
	return c.feed.Latest(ctx, &quotesv1.LatestRequest{Symbol: symbol})
}

// Publish sends quotes on one client stream and returns the server's count.
func (c *Client) Publish(ctx context.Context, quotes []*quotesv1.Quote) (*quotesv1.PublishSummary, error) {
	st, err := c.feed.Publish(ctx)
	if err != nil {
		return nil, err
	}
	for _, q := range quotes {
		if err := st.Send(q); err != nil {
			// Send reports EOF when the server already ended the stream; the
			// real reason is the status CloseAndRecv returns.
			if errors.Is(err, io.EOF) {
				_, rerr := st.CloseAndRecv()
				return nil, rerr
			}
			return nil, err
		}
	}
	return st.CloseAndRecv()
}

// Follow delivers quotes to fn until ctx ends, fn returns an error, or the
// stream fails with a code the retry policy will not retry. When the server
// goes away it waits, backs off and subscribes again. A successful quote
// resets the backoff, so a long lived follower is not penalised for failures
// from last week.
func (c *Client) Follow(ctx context.Context, symbols []string, fn func(*quotesv1.Quote) error) error {
	last := map[string]uint64{}
	failures := 0
	for {
		err := c.followOnce(ctx, symbols, last, fn, &failures)
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		var stop *fnError
		if errors.As(err, &stop) {
			return stop.err
		}
		failures++
		if !c.cfg.Retry.retryable(status.Code(err)) || failures >= max(c.cfg.Retry.MaxAttempts, 1) {
			return err
		}
		if !sleep(ctx, c.cfg.Retry.Backoff(failures, c.rnd)) {
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

type fnError struct{ err error }

func (e *fnError) Error() string { return e.err.Error() }

func (c *Client) followOnce(ctx context.Context, symbols []string, last map[string]uint64, fn func(*quotesv1.Quote) error, failures *int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	st, err := c.feed.Subscribe(ctx, &quotesv1.SubscribeRequest{Symbols: symbols})
	if err != nil {
		return err
	}
	for {
		q, err := st.Recv()
		if err != nil {
			return err
		}
		*failures = 0
		if prev := last[q.Symbol]; prev > 0 && q.Sequence > prev+1 && c.cfg.OnGap != nil {
			c.cfg.OnGap(q.Symbol, q.Sequence-prev-1)
		}
		last[q.Symbol] = q.Sequence
		if err := fn(q); err != nil {
			return &fnError{err}
		}
	}
}
