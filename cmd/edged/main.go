// Command edged serves the quote feed over gRPC.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/usmanvirk/edge-grpc/internal/feed"
	"github.com/usmanvirk/edge-grpc/internal/hub"
	"github.com/usmanvirk/edge-grpc/internal/interceptors"
	"github.com/usmanvirk/edge-grpc/internal/server"
)

type config struct {
	tokens  []string
	symbols []string
	tick    time.Duration
	drain   time.Duration
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9090", "listen address")
	symbols := flag.String("symbols", "ACME,BOLT,CRUX", "comma separated symbols to generate prices for")
	tick := flag.Duration("tick", 500*time.Millisecond, "time between price moves")
	drain := flag.Duration("drain", 10*time.Second, "how long shutdown waits for streams before cutting them")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := config{
		// Two slots so a token can be rotated without a restart-shaped gap:
		// deploy with both set, move clients over, then drop the old one.
		tokens:  []string{os.Getenv("EDGE_TOKEN"), os.Getenv("EDGE_TOKEN_NEXT")},
		symbols: strings.Split(*symbols, ","),
		tick:    *tick,
		drain:   *drain,
	}
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "edged:", err)
		os.Exit(2)
	}
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "edged:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, lis, log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func (c config) validate() error {
	have := false
	for _, t := range c.tokens {
		have = have || t != ""
	}
	if !have {
		return errors.New("set EDGE_TOKEN; refusing to serve without auth")
	}
	if len(c.symbols) == 0 || c.symbols[0] == "" {
		return errors.New("no symbols")
	}
	if c.tick <= 0 {
		return errors.New("tick must be positive")
	}
	return nil
}

// run serves on lis until ctx ends, then drains.
func run(ctx context.Context, cfg config, lis net.Listener, log *slog.Logger) error {
	h := hub.New(256)
	srv := server.New(h, interceptors.NewAuthenticator(cfg.tokens...), log, server.Options{})

	feedCtx, stopFeed := context.WithCancel(ctx)
	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		feed.Run(feedCtx, hubPublisher{h}, cfg.symbols, cfg.tick, uint64(time.Now().UnixNano()))
	}()

	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	log.Info("serving", "addr", lis.Addr().String(), "symbols", cfg.symbols)

	select {
	case err := <-served:
		stopFeed()
		<-feedDone
		h.Close()
		return err
	case <-ctx.Done():
	}

	log.Info("draining", "up_to", cfg.drain.String())
	sctx, cancel := context.WithTimeout(context.Background(), cfg.drain)
	defer cancel()
	err := srv.Shutdown(sctx)
	stopFeed()
	<-feedDone
	h.Close()
	<-served
	if errors.Is(err, context.DeadlineExceeded) {
		log.Warn("drain deadline passed, streams were cut")
		return nil
	}
	return err
}

type hubPublisher struct{ h *hub.Hub }

func (p hubPublisher) Publish(s string, price int64) error {
	_, err := p.h.Publish(s, price)
	return err
}
