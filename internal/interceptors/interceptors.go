// Package interceptors holds the server side cross-cutting pieces: request
// id, panic recovery, logging and token auth. Each comes as a unary and a
// stream version, because a gRPC server needs both and it is easy to protect
// only one.
package interceptors

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// RequestIDHeader carries the id in both directions.
	RequestIDHeader = "x-request-id"
	// AuthHeader carries "Bearer <token>".
	AuthHeader = "authorization"

	healthPrefix = "/grpc.health.v1.Health/"
	maxIDLen     = 64
)

type ctxKey struct{}

// RequestID returns the id the request id interceptor attached, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// NewRequestID is 16 random bytes in hex.
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return hex.EncodeToString(b[:])
}

// validID accepts what a caller could reasonably send and rejects anything
// that would make a log line ambiguous.
func validID(id string) bool {
	if id == "" || len(id) > maxIDLen {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

func idFrom(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(RequestIDHeader); len(v) > 0 && validID(v[0]) {
			return v[0]
		}
	}
	return NewRequestID()
}

// wrappedStream lets an interceptor swap the context a handler sees.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// UnaryRequestID reuses a valid inbound id or makes one, stores it in the
// context and returns it to the caller as a header.
func UnaryRequestID() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		id := idFrom(ctx)
		_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, id))
		return h(context.WithValue(ctx, ctxKey{}, id), req)
	}
}

func StreamRequestID() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		id := idFrom(ss.Context())
		_ = ss.SetHeader(metadata.Pairs(RequestIDHeader, id))
		return h(srv, &wrappedStream{ss, context.WithValue(ss.Context(), ctxKey{}, id)})
	}
}

// UnaryRecovery turns a panic in a handler into codes.Internal. The caller
// gets no detail; the log gets the stack.
func UnaryRecovery(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logPanic(log, ctx, info.FullMethod, r)
				resp, err = nil, status.Error(codes.Internal, "internal error")
			}
		}()
		return h(ctx, req)
	}
}

func StreamRecovery(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logPanic(log, ss.Context(), info.FullMethod, r)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return h(srv, ss)
	}
}

func logPanic(log *slog.Logger, ctx context.Context, method string, r any) {
	log.ErrorContext(ctx, "handler panicked",
		"method", method, "request_id", RequestID(ctx), "panic", r, "stack", string(debug.Stack()))
}

// UnaryLogging writes one line per call with the code and the duration.
func UnaryLogging(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := h(ctx, req)
		logCall(log, ctx, info.FullMethod, "unary", status.Code(err), time.Since(start))
		return resp, err
	}
}

func StreamLogging(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		start := time.Now()
		err := h(srv, ss)
		logCall(log, ss.Context(), info.FullMethod, "stream", status.Code(err), time.Since(start))
		return err
	}
}

func logCall(log *slog.Logger, ctx context.Context, method, kind string, code codes.Code, d time.Duration) {
	lvl := slog.LevelInfo
	if code == codes.Internal || code == codes.Unknown || code == codes.DataLoss {
		lvl = slog.LevelError
	}
	log.Log(ctx, lvl, "rpc",
		"method", method, "kind", kind, "code", code.String(),
		"ms", d.Milliseconds(), "request_id", RequestID(ctx))
}

// Authenticator checks bearer tokens. The zero value rejects everything.
type Authenticator struct {
	tokens [][]byte
}

// NewAuthenticator accepts any of the given tokens. Empty tokens are ignored,
// so an unset environment variable cannot become "accept an empty password".
func NewAuthenticator(tokens ...string) *Authenticator {
	a := &Authenticator{}
	for _, t := range tokens {
		if t != "" {
			a.tokens = append(a.tokens, []byte(t))
		}
	}
	return a
}

func (a *Authenticator) check(ctx context.Context, method string) error {
	if strings.HasPrefix(method, healthPrefix) {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing credentials")
	}
	v := md.Get(AuthHeader)
	if len(v) == 0 {
		return status.Error(codes.Unauthenticated, "missing credentials")
	}
	got, found := strings.CutPrefix(v[0], "Bearer ")
	if !found {
		return status.Error(codes.Unauthenticated, "expected a bearer token")
	}
	// Compare against every token without stopping early, so the time taken
	// does not say which one was close.
	match := 0
	for _, t := range a.tokens {
		match |= subtle.ConstantTimeCompare([]byte(got), t)
	}
	if match != 1 {
		return status.Error(codes.Unauthenticated, "bad token")
	}
	return nil
}

func (a *Authenticator) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if err := a.check(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
}

func (a *Authenticator) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if err := a.check(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return h(srv, ss)
	}
}

// Chain returns server options with the interceptors in the order that makes
// sense: recovery outermost so it catches a panic in anything below it, then
// the request id so every later line carries it, then logging so rejected
// calls are logged too, then auth.
func Chain(log *slog.Logger, auth *Authenticator) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryRecovery(log), UnaryRequestID(), UnaryLogging(log), auth.Unary()),
		grpc.ChainStreamInterceptor(StreamRecovery(log), StreamRequestID(), StreamLogging(log), auth.Stream()),
	}
}
