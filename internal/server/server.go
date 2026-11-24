// Package server implements the QuoteFeed service and owns the gRPC server
// around it: interceptors, health, serving and shutdown.
package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"github.com/usmanvirk/edge-grpc/internal/hub"
	"github.com/usmanvirk/edge-grpc/internal/interceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Options tune a Server.
type Options struct {
	// Delay is added to every Latest call. It stands in for a slow backend so
	// deadlines have something to cut short.
	Delay time.Duration
}

// Server is the QuoteFeed service plus the grpc.Server it runs on.
type Server struct {
	quotesv1.UnimplementedQuoteFeedServer

	hub    *hub.Hub
	opts   Options
	log    *slog.Logger
	gs     *grpc.Server
	health *health.Server

	drain     chan struct{} // closed when shutdown starts
	drainOnce sync.Once
}

// New builds a server. Every method except the health check requires one of
// the authenticator's tokens.
func New(h *hub.Hub, auth *interceptors.Authenticator, log *slog.Logger, opts Options) *Server {
	s := &Server{hub: h, opts: opts, log: log, health: health.NewServer(), drain: make(chan struct{})}
	s.gs = grpc.NewServer(interceptors.Chain(log, auth)...)
	quotesv1.RegisterQuoteFeedServer(s.gs, s)
	healthpb.RegisterHealthServer(s.gs, s.health)
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	s.health.SetServingStatus(quotesv1.QuoteFeed_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	return s
}

// Serve blocks until the listener is closed or the server stops.
func (s *Server) Serve(lis net.Listener) error { return s.gs.Serve(lis) }

// Shutdown marks the server not serving, tells open streams to finish, then
// waits for every call in flight. Subscribe and Watch streams flush what they
// have queued and end with Unavailable. When ctx ends first it stops hard and
// returns the context's error.
//
// GracefulStop alone is not enough: it waits for streams, and a subscription
// has no natural end.
func (s *Server) Shutdown(ctx context.Context) error {
	s.health.Shutdown()
	s.drainOnce.Do(func() { close(s.drain) })
	done := make(chan struct{})
	go func() {
		s.gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.gs.Stop()
		<-done
		return ctx.Err()
	}
}

func (s *Server) Latest(ctx context.Context, req *quotesv1.LatestRequest) (*quotesv1.Quote, error) {
	if req.GetSymbol() == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}
	if s.opts.Delay > 0 {
		t := time.NewTimer(s.opts.Delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	q, err := s.hub.Latest(req.Symbol)
	if errors.Is(err, hub.ErrUnknownSymbol) {
		return nil, status.Errorf(codes.NotFound, "no quote for %q", req.Symbol)
	}
	return q, err
}

func (s *Server) Subscribe(req *quotesv1.SubscribeRequest, stream grpc.ServerStreamingServer[quotesv1.Quote]) error {
	for _, sym := range req.Symbols {
		if sym == "" {
			return status.Error(codes.InvalidArgument, "empty symbol in list")
		}
	}
	sub, err := s.hub.Subscribe(req.Symbols...)
	if err != nil {
		return status.Error(codes.Unavailable, "feed closed")
	}
	defer sub.Close()
	ctx := stream.Context()
	for {
		select {
		case q, ok := <-sub.C:
			if !ok {
				return subErr(sub)
			}
			if err := stream.Send(q); err != nil {
				return err
			}
		case <-s.drain:
			return s.flush(sub, stream, nil)
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

func (s *Server) Publish(stream grpc.ClientStreamingServer[quotesv1.Quote, quotesv1.PublishSummary]) error {
	var sum quotesv1.PublishSummary
	for {
		q, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&sum)
		}
		if err != nil {
			return err
		}
		switch _, err := s.hub.Publish(q.GetSymbol(), q.GetPriceMinor()); {
		case err == nil:
			sum.Accepted++
		case errors.Is(err, hub.ErrBadQuote):
			sum.Rejected++
		default:
			return status.Error(codes.Unavailable, "feed closed")
		}
	}
}

func (s *Server) Watch(stream grpc.BidiStreamingServer[quotesv1.WatchRequest, quotesv1.Quote]) error {
	sub, err := s.hub.Subscribe()
	if err != nil {
		return status.Error(codes.Unavailable, "feed closed")
	}
	defer sub.Close()

	var (
		mu      sync.Mutex
		watched = map[string]struct{}{}
	)
	// The reader ends when Recv fails, which it does as soon as the handler
	// returns and the stream's context is cancelled.
	recvDone := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvDone <- err
				return
			}
			mu.Lock()
			switch a := req.Action.(type) {
			case *quotesv1.WatchRequest_Add:
				if a.Add != "" {
					watched[a.Add] = struct{}{}
				}
			case *quotesv1.WatchRequest_Remove:
				delete(watched, a.Remove)
			}
			mu.Unlock()
		}
	}()

	ctx := stream.Context()
	for {
		select {
		case q, ok := <-sub.C:
			if !ok {
				return subErr(sub)
			}
			mu.Lock()
			_, want := watched[q.Symbol]
			mu.Unlock()
			if !want {
				continue
			}
			if err := stream.Send(q); err != nil {
				return err
			}
		case <-s.drain:
			return s.flush(sub, stream, func(q *quotesv1.Quote) bool {
				mu.Lock()
				defer mu.Unlock()
				_, want := watched[q.Symbol]
				return want
			})
		case err := <-recvDone:
			if errors.Is(err, io.EOF) {
				return nil // the caller finished sending and is done
			}
			return err
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

// flush sends whatever is already queued for a subscriber, without waiting for
// more, then ends the stream as Unavailable.
func (s *Server) flush(sub *hub.Sub, stream grpc.ServerStream, keep func(*quotesv1.Quote) bool) error {
	for {
		select {
		case q, ok := <-sub.C:
			if !ok {
				return status.Error(codes.Unavailable, "server draining")
			}
			if keep != nil && !keep(q) {
				continue
			}
			if err := stream.SendMsg(q); err != nil {
				return err
			}
		default:
			return status.Error(codes.Unavailable, "server draining")
		}
	}
}

func subErr(sub *hub.Sub) error {
	switch err := sub.Err(); {
	case errors.Is(err, hub.ErrSlow):
		return status.Error(codes.ResourceExhausted, "reader too slow, subscription dropped")
	default:
		return status.Error(codes.Unavailable, "feed closed")
	}
}
