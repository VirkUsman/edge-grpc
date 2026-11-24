// Package feed makes up prices so the service has something to serve.
package feed

import (
	"context"
	"math/rand/v2"
	"time"
)

// Publisher is the part of the hub the feed needs.
type Publisher interface {
	Publish(symbol string, priceMinor int64) error
}

// Start prices, in cents.
const startPrice = 10_000

// Run moves each symbol by a random step every interval until ctx ends. A
// symbol's first price is published immediately, so Latest works at once.
// Steps are up to 0.5% either way and a price never drops below 1 cent.
func Run(ctx context.Context, p Publisher, symbols []string, every time.Duration, seed uint64) {
	rnd := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	prices := make(map[string]int64, len(symbols))
	for _, s := range symbols {
		prices[s] = startPrice
		p.Publish(s, startPrice)
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, s := range symbols {
				prices[s] = Step(prices[s], rnd.Float64())
				p.Publish(s, prices[s])
			}
		}
	}
}

// Step moves a price by up to 0.5% for a roll in [0,1): 0 is the biggest fall,
// just under 1 the biggest rise.
func Step(price int64, roll float64) int64 {
	next := price + int64(float64(price)*0.005*(2*roll-1))
	if next < 1 {
		return 1
	}
	return next
}
