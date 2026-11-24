package interceptors

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func inCtx(kv ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(kv...))
}

func unaryInfo() *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{FullMethod: "/quotes.v1.QuoteFeed/Latest"}
}

func ok(context.Context, any) (any, error) { return "ok", nil }

func TestValidID(t *testing.T) {
	cases := map[string]bool{
		"abc-123_X.y":           true,
		"":                      false,
		"has space":             false,
		"new\nline":             false,
		"semi;colon":            false,
		strings.Repeat("a", 64): true,
		strings.Repeat("a", 65): false,
	}
	for in, want := range cases {
		if got := validID(in); got != want {
			t.Errorf("validID(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestNewRequestIDIsUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewRequestID()
		if len(id) != 32 || seen[id] {
			t.Fatalf("bad or repeated id %q", id)
		}
		seen[id] = true
	}
}

func TestRequestIDKeepsAValidInboundID(t *testing.T) {
	var got string
	_, err := UnaryRequestID()(inCtx(RequestIDHeader, "from-caller-1"), nil, unaryInfo(),
		func(ctx context.Context, _ any) (any, error) { got = RequestID(ctx); return nil, nil })
	if err != nil || got != "from-caller-1" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestRequestIDReplacesAnInvalidInboundID(t *testing.T) {
	var got string
	UnaryRequestID()(inCtx(RequestIDHeader, "bad id\n"), nil, unaryInfo(),
		func(ctx context.Context, _ any) (any, error) { got = RequestID(ctx); return nil, nil })
	if got == "" || got == "bad id\n" || len(got) != 32 {
		t.Fatalf("got %q", got)
	}
}

func TestRequestIDMadeWhenAbsent(t *testing.T) {
	var got string
	UnaryRequestID()(context.Background(), nil, unaryInfo(),
		func(ctx context.Context, _ any) (any, error) { got = RequestID(ctx); return nil, nil })
	if len(got) != 32 {
		t.Fatalf("got %q", got)
	}
}

func TestRequestIDEmptyOutsideAnInterceptor(t *testing.T) {
	if RequestID(context.Background()) != "" {
		t.Fatal("expected empty")
	}
}

func TestRecoveryTurnsPanicIntoInternal(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	_, err := UnaryRecovery(log)(context.Background(), nil, unaryInfo(),
		func(context.Context, any) (any, error) { panic("boom") })
	if status.Code(err) != codes.Internal {
		t.Fatalf("code %v", status.Code(err))
	}
	if strings.Contains(err.Error(), "boom") {
		t.Fatalf("panic text leaked to the caller: %v", err)
	}
	if !strings.Contains(buf.String(), "boom") || !strings.Contains(buf.String(), "stack") {
		t.Fatalf("log lacks the panic or stack: %s", buf.String())
	}
}

func TestRecoveryPassesNormalResultsThrough(t *testing.T) {
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	resp, err := UnaryRecovery(log)(context.Background(), nil, unaryInfo(), ok)
	if resp != "ok" || err != nil {
		t.Fatalf("%v %v", resp, err)
	}
}

func TestRecoveryPassesHandlerErrorsThrough(t *testing.T) {
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	want := status.Error(codes.NotFound, "x")
	_, err := UnaryRecovery(log)(context.Background(), nil, unaryInfo(),
		func(context.Context, any) (any, error) { return nil, want })
	if err != want {
		t.Fatalf("got %v", err)
	}
}

func TestLoggingRecordsCodeAndRequestID(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	ctx := context.WithValue(context.Background(), ctxKey{}, "rid-9")
	UnaryLogging(log)(ctx, nil, unaryInfo(),
		func(context.Context, any) (any, error) { return nil, status.Error(codes.NotFound, "x") })
	out := buf.String()
	for _, want := range []string{"code=NotFound", "request_id=rid-9", "method=/quotes.v1.QuoteFeed/Latest", "level=INFO"} {
		if !strings.Contains(out, want) {
			t.Errorf("log %q lacks %q", out, want)
		}
	}
}

func TestLoggingMarksInternalAsError(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	UnaryLogging(log)(context.Background(), nil, unaryInfo(),
		func(context.Context, any) (any, error) { return nil, status.Error(codes.Internal, "x") })
	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Fatal(buf.String())
	}
}

func TestAuthAcceptsAKnownToken(t *testing.T) {
	a := NewAuthenticator("s3cret")
	resp, err := a.Unary()(inCtx(AuthHeader, "Bearer s3cret"), nil, unaryInfo(), ok)
	if resp != "ok" || err != nil {
		t.Fatalf("%v %v", resp, err)
	}
}

func TestAuthAcceptsEitherOfTwoTokens(t *testing.T) {
	a := NewAuthenticator("old", "new")
	for _, tok := range []string{"old", "new"} {
		if _, err := a.Unary()(inCtx(AuthHeader, "Bearer "+tok), nil, unaryInfo(), ok); err != nil {
			t.Errorf("%s rejected: %v", tok, err)
		}
	}
}

func TestAuthRejections(t *testing.T) {
	a := NewAuthenticator("s3cret")
	cases := map[string]context.Context{
		"no metadata":   context.Background(),
		"no header":     inCtx("other", "x"),
		"wrong scheme":  inCtx(AuthHeader, "Basic s3cret"),
		"wrong token":   inCtx(AuthHeader, "Bearer nope"),
		"prefix only":   inCtx(AuthHeader, "Bearer "),
		"longer token":  inCtx(AuthHeader, "Bearer s3cretX"),
		"shorter token": inCtx(AuthHeader, "Bearer s3cre"),
	}
	for name, ctx := range cases {
		_, err := a.Unary()(ctx, nil, unaryInfo(), ok)
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: code %v", name, status.Code(err))
		}
	}
}

func TestEmptyTokenIsNotAPassword(t *testing.T) {
	a := NewAuthenticator("")
	if _, err := a.Unary()(inCtx(AuthHeader, "Bearer "), nil, unaryInfo(), ok); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("empty bearer accepted: %v", err)
	}
}

func TestZeroValueAuthenticatorRejectsEverything(t *testing.T) {
	var a Authenticator
	if _, err := a.Unary()(inCtx(AuthHeader, "Bearer x"), nil, unaryInfo(), ok); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code %v", status.Code(err))
	}
}

func TestHealthIsExemptFromAuth(t *testing.T) {
	a := NewAuthenticator("s3cret")
	info := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	if _, err := a.Unary()(context.Background(), nil, info, ok); err != nil {
		t.Fatalf("health check refused: %v", err)
	}
}

func TestAuthDoesNotCallTheHandlerOnFailure(t *testing.T) {
	called := false
	NewAuthenticator("s").Unary()(context.Background(), nil, unaryInfo(),
		func(context.Context, any) (any, error) { called = true; return nil, nil })
	if called {
		t.Fatal("handler ran for an unauthenticated call")
	}
}

// fakeStream is the least a ServerStream needs for the stream interceptors.
type fakeStream struct {
	grpc.ServerStream
	ctx    context.Context
	header metadata.MD
}

func (f *fakeStream) Context() context.Context { return f.ctx }
func (f *fakeStream) SetHeader(md metadata.MD) error {
	f.header = metadata.Join(f.header, md)
	return nil
}

func streamInfo() *grpc.StreamServerInfo {
	return &grpc.StreamServerInfo{FullMethod: "/quotes.v1.QuoteFeed/Subscribe"}
}

func TestStreamRequestIDSetsHeaderAndContext(t *testing.T) {
	fs := &fakeStream{ctx: inCtx(RequestIDHeader, "stream-1")}
	var seen string
	err := StreamRequestID()(nil, fs, streamInfo(), func(_ any, ss grpc.ServerStream) error {
		seen = RequestID(ss.Context())
		return nil
	})
	if err != nil || seen != "stream-1" {
		t.Fatalf("%q %v", seen, err)
	}
	if got := fs.header.Get(RequestIDHeader); len(got) != 1 || got[0] != "stream-1" {
		t.Fatalf("header %v", got)
	}
}

func TestStreamRecovery(t *testing.T) {
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	err := StreamRecovery(log)(nil, &fakeStream{ctx: context.Background()}, streamInfo(),
		func(any, grpc.ServerStream) error { panic("kaboom") })
	if status.Code(err) != codes.Internal {
		t.Fatalf("code %v", status.Code(err))
	}
}

func TestStreamAuthRejectsAndAccepts(t *testing.T) {
	a := NewAuthenticator("tok")
	h := func(any, grpc.ServerStream) error { return nil }
	if err := a.Stream()(nil, &fakeStream{ctx: context.Background()}, streamInfo(), h); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code %v", status.Code(err))
	}
	if err := a.Stream()(nil, &fakeStream{ctx: inCtx(AuthHeader, "Bearer tok")}, streamInfo(), h); err != nil {
		t.Fatal(err)
	}
}

func TestStreamLoggingRecordsKind(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	StreamLogging(log)(nil, &fakeStream{ctx: context.Background()}, streamInfo(),
		func(any, grpc.ServerStream) error { return nil })
	if !strings.Contains(buf.String(), "kind=stream") || !strings.Contains(buf.String(), "code=OK") {
		t.Fatal(buf.String())
	}
}
