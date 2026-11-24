// Package hub holds the latest quote per symbol and fans new quotes out to
// subscribers.
package hub

import (
	"errors"
	"sync"
	"time"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrUnknownSymbol = errors.New("hub: unknown symbol")
	ErrBadQuote      = errors.New("hub: quote needs a symbol and a positive price")
	ErrClosed        = errors.New("hub: closed")
)

// Hub is safe for concurrent use.
type Hub struct {
	mu     sync.Mutex
	last   map[string]*quotesv1.Quote
	subs   map[*Sub]struct{}
	buffer int
	closed bool
	now    func() time.Time
}

// Sub is one subscriber. Read C until it closes; Err says why it closed.
type Sub struct {
	C       <-chan *quotesv1.Quote
	c       chan *quotesv1.Quote
	symbols map[string]struct{} // nil means every symbol
	hub     *Hub
	err     error
}

// ErrSlow closes a subscriber whose buffer filled. Dropping it is cheaper
// than letting one slow reader hold up every publisher.
var ErrSlow = errors.New("hub: subscriber too slow, dropped")

// New returns a hub whose subscribers buffer up to buffer quotes.
func New(buffer int) *Hub {
	if buffer < 1 {
		buffer = 1
	}
	return &Hub{
		last:   map[string]*quotesv1.Quote{},
		subs:   map[*Sub]struct{}{},
		buffer: buffer,
		now:    time.Now,
	}
}

// SetClock replaces the clock. For tests.
func (h *Hub) SetClock(now func() time.Time) { h.now = now }

// Publish stamps the quote with the next sequence number for its symbol and
// delivers it. The stored copy is the one subscribers receive, so callers
// must not mutate it; Latest hands out clones for that reason.
func (h *Hub) Publish(symbol string, priceMinor int64) (*quotesv1.Quote, error) {
	if symbol == "" || priceMinor <= 0 {
		return nil, ErrBadQuote
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	var seq uint64 = 1
	if prev, ok := h.last[symbol]; ok {
		seq = prev.Sequence + 1
	}
	q := &quotesv1.Quote{
		Symbol:     symbol,
		PriceMinor: priceMinor,
		Sequence:   seq,
		At:         timestamppb.New(h.now()),
	}
	h.last[symbol] = q
	for s := range h.subs {
		if !s.wants(symbol) {
			continue
		}
		select {
		case s.c <- q:
		default:
			s.drop(ErrSlow)
		}
	}
	return q, nil
}

// Latest returns a copy of the most recent quote for a symbol.
func (h *Hub) Latest(symbol string) (*quotesv1.Quote, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	q, ok := h.last[symbol]
	if !ok {
		return nil, ErrUnknownSymbol
	}
	return cloneQuote(q), nil
}

// Subscribe registers a subscriber for the given symbols, or every symbol when
// none are given. Call Close on it when done.
func (h *Hub) Subscribe(symbols ...string) (*Sub, error) {
	c := make(chan *quotesv1.Quote, h.buffer)
	s := &Sub{C: c, c: c, hub: h}
	if len(symbols) > 0 {
		s.symbols = make(map[string]struct{}, len(symbols))
		for _, sym := range symbols {
			s.symbols[sym] = struct{}{}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	h.subs[s] = struct{}{}
	return s, nil
}

// Subscribers is the number of live subscribers.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// Close ends every subscription. Further publishes fail.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for s := range h.subs {
		s.drop(ErrClosed)
	}
}

// Err reports why the channel closed, once it has. Nil while it is open.
func (s *Sub) Err() error {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	return s.err
}

// Close unsubscribes. Safe to call more than once.
func (s *Sub) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	if _, ok := s.hub.subs[s]; ok {
		delete(s.hub.subs, s)
		close(s.c)
	}
}

func (s *Sub) wants(symbol string) bool {
	if s.symbols == nil {
		return true
	}
	_, ok := s.symbols[symbol]
	return ok
}

// drop closes the subscriber from inside Publish or Close, which already hold
// the hub lock.
func (s *Sub) drop(err error) {
	if _, ok := s.hub.subs[s]; !ok {
		return
	}
	delete(s.hub.subs, s)
	s.err = err
	close(s.c)
}

func cloneQuote(q *quotesv1.Quote) *quotesv1.Quote {
	return &quotesv1.Quote{
		Symbol:     q.Symbol,
		PriceMinor: q.PriceMinor,
		Sequence:   q.Sequence,
		At:         timestamppb.New(q.At.AsTime()),
	}
}
