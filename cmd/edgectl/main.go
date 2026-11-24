// Command edgectl is a small client for edged.
//
//	edgectl latest ACME
//	edgectl follow ACME BOLT
//	edgectl publish ACME=10250 BOLT=990
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"github.com/usmanvirk/edge-grpc/internal/client"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9090", "server address")
	useTLS := flag.Bool("tls", false, "use TLS (edged itself serves plaintext)")
	timeout := flag.Duration("timeout", 3*time.Second, "deadline for latest and publish")
	flag.Parse()
	if err := run(os.Stdout, *addr, !*useTLS, *timeout, os.Getenv("EDGE_TOKEN"), flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "edgectl:", err)
		os.Exit(1)
	}
}

func run(out io.Writer, addr string, plaintext bool, timeout time.Duration, token string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: edgectl latest SYMBOL | follow SYMBOLS | publish PAIRS")
	}
	c, err := client.Dial(addr, client.Config{
		Token: token, Plaintext: plaintext, Retry: client.DefaultRetry,
		OnGap: func(sym string, n uint64) { fmt.Fprintf(out, "# missed %d quotes for %s\n", n, sym) },
	})
	if err != nil {
		return err
	}
	defer c.Close()

	switch args[0] {
	case "latest":
		if len(args) != 2 {
			return fmt.Errorf("latest takes one symbol")
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		q, err := c.Latest(ctx, args[1])
		if err != nil {
			return err
		}
		printQuote(out, q)
	case "follow":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		err = c.Follow(ctx, args[1:], func(q *quotesv1.Quote) error { printQuote(out, q); return nil })
		if ctx.Err() != nil {
			return nil // ctrl-c is how this command is meant to end
		}
		return err
	case "publish":
		qs, err := parsePairs(args[1:])
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		sum, err := c.Publish(ctx, qs)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "accepted %d, rejected %d\n", sum.Accepted, sum.Rejected)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	return nil
}

func parsePairs(args []string) ([]*quotesv1.Quote, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("publish takes SYMBOL=PRICE pairs, price in cents")
	}
	var out []*quotesv1.Quote
	for _, a := range args {
		sym, p, ok := strings.Cut(a, "=")
		if !ok || sym == "" {
			return nil, fmt.Errorf("%q is not SYMBOL=PRICE", a)
		}
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q: price must be a whole number of cents", a)
		}
		out = append(out, &quotesv1.Quote{Symbol: sym, PriceMinor: n})
	}
	return out, nil
}

func printQuote(w io.Writer, q *quotesv1.Quote) {
	fmt.Fprintf(w, "%s #%d %d.%02d %s\n", q.Symbol, q.Sequence, q.PriceMinor/100, q.PriceMinor%100, q.At.AsTime().Format("15:04:05.000"))
}
