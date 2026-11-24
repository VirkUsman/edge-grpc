# edge-grpc

_Built with AI assistance._

A price feed over gRPC in Go, with all four call shapes, deadlines that reach
the handler, interceptors for auth, request ids, panic recovery and logging,
health checking, and a shutdown that ends open streams instead of waiting on
them forever.

Go 1.26, grpc-go 1.85. No database, nothing to install but the Go toolchain.

```
proto/quotes/v1/        the contract
gen/quotes/v1/      generated code, committed
internal/hub/       latest quote per symbol, filtered fan-out to subscribers
internal/server/    the four handlers, health, Shutdown
internal/interceptors/  request id, recovery, logging, bearer auth
internal/client/    token and id on every call, retry with backoff, Follow
internal/feed/      the random walk that gives the service prices
cmd/edged/          the daemon
cmd/edgectl/        latest, follow, publish
```

## Run it

    EDGE_TOKEN=dev go run ./cmd/edged                 # terminal one
    EDGE_TOKEN=dev go run ./cmd/edgectl follow ACME   # terminal two
    EDGE_TOKEN=dev go run ./cmd/edgectl latest BOLT
    EDGE_TOKEN=dev go run ./cmd/edgectl publish ACME=10250

`edged` refuses to start without `EDGE_TOKEN`. Prices are integer cents.
Ctrl-C starts a drain of up to 10 seconds.

## The four shapes

| RPC | Shape | What it is for |
|---|---|---|
| `Latest` | unary | the current quote for one symbol |
| `Subscribe` | server stream | quotes as they are published |
| `Publish` | client stream | a feed handler pushes quotes, gets a count back |
| `Watch` | bidirectional | change the set of symbols while quotes keep flowing |

Each quote carries a per symbol sequence number starting at 1. A reader that
sees a jump knows it missed something, and `Follow` reports it through
`OnGap`.

## Deadlines and cancellation

The handler for `Latest` waits on the caller's context, so a 60ms deadline
against a 2 second backend returns `DeadlineExceeded` in about 60ms and frees
the handler. `TestServerSeesTheCallersDeadlinePropagated` proves the handler is
actually released, by shutting down with a one second budget while a two
second call has been abandoned. Streams end the same way on cancel, and the
subscription is removed from the hub when they do.

## Interceptors

Order is recovery, request id, logging, auth, each in a unary and a stream
version. Recovery is outermost so a panic in any of the others is caught. The
request id comes before logging so a rejected call is still logged with an id.
A caller's id is kept only if it is 1 to 64 characters of letters, digits,
`-`, `_` and `.`; anything else is replaced, so an id cannot split a log line.
Tokens are compared in constant time against every configured token, and an
empty token is dropped so an unset variable cannot become a password.
`EDGE_TOKEN_NEXT` is a second accepted token for rotation. The health service
is exempt from auth so a load balancer can probe it.

## Shutdown

`Shutdown` flips health to NOT_SERVING, tells `Subscribe` and `Watch` streams
to flush what is queued and end with `Unavailable`, then calls `GracefulStop`.
If the context ends first it calls `Stop` and returns the context's error.

This was wrong in the first version. It called `GracefulStop` alone, which
waits for every stream, and a subscription has no end of its own, so shutdown
sat until its deadline and cut everything off. The test that caught it opened
one stream and waited two seconds for a result that never came. The fix is in
the commit titled "Fix shutdown hanging on open streams".

## Client

Every call carries the token and a request id. The id is made once per call
and a retried call keeps it, which is why the header interceptor sits outside
the retry one. Retry is exponential with jitter and defaults to `Unavailable`
only, because that is the one code meaning the call did not run. It will not
sleep past the caller's deadline and it stops when the context is cancelled.
`Follow` resubscribes after a failure, resets its backoff once a quote
arrives, and passes `OnGap` the number of quotes it missed.

## Tests

    go test -race ./...

133 tests, all with the race detector, no network and no external service.
Server and client tests use an in memory `bufconn` listener, so the real
interceptors, codec and status codes run. `cmd/edged` tests run the whole
daemon on a real `127.0.0.1` port. Every package runs `goleak` in `TestMain`,
so a test that leaves a goroutine behind fails the package.

The reconnect tests start a second server on a new listener behind the same
dial address, shut the first one down, and check that `Follow` lands on the
second and reports no gap, or exactly 2 missed when two quotes were published
in between.

What ran for this repo: build, `go vet`, `staticcheck`, `buf lint`,
`go test -race -count=5` over the internal packages, and a `buf generate` that
left `gen/` unchanged. `ci.yml` has not run on GitHub, since the repo has no
remote yet.

## Regenerating the code

    go install github.com/bufbuild/buf/cmd/buf@v1.47.2
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.2
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
    make proto        # buf lint, then buf generate

`buf.gen.yaml` uses the two local plugins above, so no network and no protoc.

## What it does not do

No TLS in `edged`. It serves plaintext and expects something in front of it;
the client has TLS but that path has no test.

Nothing is stored. Restart the server and every sequence number starts at 1
again, which `Follow` tolerates but cannot tell apart from a reset.

A `Publish` stream is not told to leave on shutdown. A client holding one open
is cut when the drain deadline passes, and loses its summary.

No replay. `Follow` knows how many quotes it missed and cannot get them back.

A slow subscriber is dropped with `ResourceExhausted` rather than slowing the
publishers. That is a choice, and a reader that needs every quote would want a
different one.

Retry is only for unary calls and for reopening a `Follow`. `Publish` is not
retried, because it is not idempotent.
