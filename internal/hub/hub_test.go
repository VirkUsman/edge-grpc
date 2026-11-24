package hub

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func recv(t *testing.T, s *Sub) (string, uint64, bool) {
	t.Helper()
	select {
	case q, ok := <-s.C:
		if !ok {
			return "", 0, false
		}
		return q.Symbol, q.Sequence, true
	case <-time.After(time.Second):
		t.Fatal("no quote within a second")
	}
	return "", 0, false
}

func TestPublishRejectsBadQuotes(t *testing.T) {
	h := New(4)
	for _, c := range []struct {
		sym   string
		price int64
	}{{"", 100}, {"ACME", 0}, {"ACME", -5}} {
		if _, err := h.Publish(c.sym, c.price); err != ErrBadQuote {
			t.Errorf("Publish(%q,%d) = %v, want ErrBadQuote", c.sym, c.price, err)
		}
	}
}

func TestSequenceStartsAtOneAndCountsPerSymbol(t *testing.T) {
	h := New(4)
	for i, want := range []uint64{1, 2, 3} {
		q, err := h.Publish("ACME", int64(100+i))
		if err != nil || q.Sequence != want {
			t.Fatalf("ACME #%d: seq %d err %v, want %d", i, q.GetSequence(), err, want)
		}
	}
	q, _ := h.Publish("BOLT", 5)
	if q.Sequence != 1 {
		t.Fatalf("BOLT starts at %d, want 1", q.Sequence)
	}
}

func TestLatestUnknownSymbol(t *testing.T) {
	if _, err := New(1).Latest("NOPE"); err != ErrUnknownSymbol {
		t.Fatalf("got %v", err)
	}
}

func TestLatestReturnsMostRecent(t *testing.T) {
	h := New(1)
	h.Publish("ACME", 100)
	h.Publish("ACME", 130)
	q, err := h.Latest("ACME")
	if err != nil || q.PriceMinor != 130 || q.Sequence != 2 {
		t.Fatalf("got %v %v", q, err)
	}
}

func TestLatestReturnsACopy(t *testing.T) {
	h := New(1)
	h.Publish("ACME", 100)
	a, _ := h.Latest("ACME")
	a.PriceMinor = 1
	b, _ := h.Latest("ACME")
	if b.PriceMinor != 100 {
		t.Fatalf("mutating a result changed the store: %d", b.PriceMinor)
	}
}

func TestClockStampsQuotes(t *testing.T) {
	h := New(1)
	at := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	h.SetClock(func() time.Time { return at })
	q, _ := h.Publish("ACME", 1)
	if !q.At.AsTime().Equal(at) {
		t.Fatalf("stamped %v", q.At.AsTime())
	}
}

func TestSubscriberGetsOnlyItsSymbols(t *testing.T) {
	h := New(8)
	s, _ := h.Subscribe("ACME")
	defer s.Close()
	h.Publish("BOLT", 1)
	h.Publish("ACME", 2)
	sym, _, _ := recv(t, s)
	if sym != "ACME" {
		t.Fatalf("got %s", sym)
	}
}

func TestEmptySubscribeMeansEverything(t *testing.T) {
	h := New(8)
	s, _ := h.Subscribe()
	defer s.Close()
	h.Publish("BOLT", 1)
	h.Publish("ACME", 2)
	a, _, _ := recv(t, s)
	b, _, _ := recv(t, s)
	if a != "BOLT" || b != "ACME" {
		t.Fatalf("got %s %s", a, b)
	}
}

func TestSlowSubscriberIsDroppedNotWaitedFor(t *testing.T) {
	h := New(2)
	slow, _ := h.Subscribe()
	fast, _ := h.Subscribe()
	defer fast.Close()
	for i := 0; i < 5; i++ {
		h.Publish("ACME", int64(i+1))
		recv(t, fast)
	}
	// Drain what the slow one buffered, then it must be closed with ErrSlow.
	n := 0
	for range slow.C {
		n++
	}
	if n != 2 {
		t.Errorf("slow subscriber buffered %d, want 2", n)
	}
	if slow.Err() != ErrSlow {
		t.Errorf("Err = %v", slow.Err())
	}
	if h.Subscribers() != 1 {
		t.Errorf("subscribers = %d", h.Subscribers())
	}
}

func TestCloseSubscriberIsIdempotent(t *testing.T) {
	h := New(1)
	s, _ := h.Subscribe()
	s.Close()
	s.Close()
	if h.Subscribers() != 0 {
		t.Fatal("still registered")
	}
	if _, ok := <-s.C; ok {
		t.Fatal("channel not closed")
	}
}

func TestPublishAfterSubscriberCloseDoesNotPanic(t *testing.T) {
	h := New(1)
	s, _ := h.Subscribe()
	s.Close()
	if _, err := h.Publish("ACME", 1); err != nil {
		t.Fatal(err)
	}
}

func TestHubCloseEndsSubscribersAndRefusesPublishes(t *testing.T) {
	h := New(1)
	s, _ := h.Subscribe()
	h.Close()
	h.Close()
	if _, ok := <-s.C; ok {
		t.Fatal("channel open after Close")
	}
	if s.Err() != ErrClosed {
		t.Fatalf("Err = %v", s.Err())
	}
	if _, err := h.Publish("ACME", 1); err != ErrClosed {
		t.Fatalf("Publish = %v", err)
	}
	if _, err := h.Subscribe(); err != ErrClosed {
		t.Fatalf("Subscribe = %v", err)
	}
}

func TestWatchChangesTheSetInFlight(t *testing.T) {
	h := New(8)
	s, _ := h.Subscribe("ACME")
	defer s.Close()
	h.Publish("BOLT", 1)
	s.Watch("BOLT")
	h.Publish("ACME", 2)
	h.Publish("BOLT", 3)
	sym, seq, _ := recv(t, s)
	if sym != "BOLT" || seq != 2 {
		t.Fatalf("got %s #%d", sym, seq)
	}
}

func TestWatchWithNothingMeansEverything(t *testing.T) {
	h := New(8)
	s, _ := h.Subscribe("ACME")
	defer s.Close()
	s.Watch()
	h.Publish("BOLT", 1)
	if sym, _, _ := recv(t, s); sym != "BOLT" {
		t.Fatalf("got %s", sym)
	}
}

func TestConcurrentPublishersKeepSequenceGapless(t *testing.T) {
	h := New(1000)
	s, _ := h.Subscribe("ACME")
	defer s.Close()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				h.Publish("ACME", 10)
			}
		}()
	}
	wg.Wait()
	var want uint64 = 1
	for i := 0; i < 400; i++ {
		_, seq, ok := recv(t, s)
		if !ok || seq != want {
			t.Fatalf("message %d: seq %d, want %d", i, seq, want)
		}
		want++
	}
}

func TestManySubscribersAllSeeEveryQuote(t *testing.T) {
	h := New(16)
	var subs []*Sub
	for i := 0; i < 10; i++ {
		s, _ := h.Subscribe()
		defer s.Close()
		subs = append(subs, s)
	}
	for i := 0; i < 5; i++ {
		h.Publish("ACME", int64(i+1))
	}
	for i, s := range subs {
		for want := uint64(1); want <= 5; want++ {
			if _, seq, _ := recv(t, s); seq != want {
				t.Fatalf("sub %d: seq %d, want %d", i, seq, want)
			}
		}
	}
}
