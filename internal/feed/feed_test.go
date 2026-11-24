package feed

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type rec struct {
	mu  sync.Mutex
	got []struct {
		sym   string
		price int64
	}
}

func (r *rec) Publish(s string, p int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, struct {
		sym   string
		price int64
	}{s, p})
	return nil
}

func (r *rec) n() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func TestStepBounds(t *testing.T) {
	if got := Step(10_000, 0); got != 9_950 {
		t.Errorf("biggest fall: %d", got)
	}
	if got := Step(10_000, 0.5); got != 10_000 {
		t.Errorf("no move: %d", got)
	}
	if got := Step(10_000, 0.999999); got < 10_049 || got > 10_050 {
		t.Errorf("biggest rise: %d", got)
	}
}

func TestStepNeverReachesZero(t *testing.T) {
	p := int64(1)
	for i := 0; i < 1000; i++ {
		p = Step(p, 0)
		if p < 1 {
			t.Fatalf("price %d", p)
		}
	}
}

func TestRunPublishesEverySymbolImmediately(t *testing.T) {
	r := &rec{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, r, []string{"A", "B", "C"}, time.Hour, 1); close(done) }()
	deadline := time.Now().Add(time.Second)
	for r.n() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if r.n() != 3 {
		t.Fatalf("%d publishes, want 3", r.n())
	}
	for _, g := range r.got {
		if g.price != startPrice {
			t.Errorf("%s opened at %d", g.sym, g.price)
		}
	}
}

func TestRunKeepsTicking(t *testing.T) {
	r := &rec{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, r, []string{"A"}, 5*time.Millisecond, 1); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for r.n() < 6 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	if r.n() < 6 {
		t.Fatalf("only %d publishes", r.n())
	}
}

func TestRunStopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, &rec{}, []string{"A"}, time.Millisecond, 1); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return")
	}
}

func TestSameSeedSamePrices(t *testing.T) {
	run := func() []int64 {
		r := &rec{}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { Run(ctx, r, []string{"A"}, time.Millisecond, 42); close(done) }()
		for r.n() < 8 {
			time.Sleep(time.Millisecond)
		}
		cancel()
		<-done
		var out []int64
		for _, g := range r.got[:8] {
			out = append(out, g.price)
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("diverged at %d: %v vs %v", i, a, b)
		}
	}
}
