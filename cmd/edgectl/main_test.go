package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	quotesv1 "github.com/usmanvirk/edge-grpc/gen/quotes/v1"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestParsePairs(t *testing.T) {
	qs, err := parsePairs([]string{"ACME=10250", "BOLT=990"})
	if err != nil || len(qs) != 2 || qs[0].Symbol != "ACME" || qs[0].PriceMinor != 10250 || qs[1].PriceMinor != 990 {
		t.Fatalf("%v %v", qs, err)
	}
}

func TestParsePairsRejectsBadInput(t *testing.T) {
	for _, in := range [][]string{nil, {"ACME"}, {"=5"}, {"ACME=ten"}, {"ACME=1.50"}} {
		if _, err := parsePairs(in); err == nil {
			t.Errorf("%v accepted", in)
		}
	}
}

func TestPrintQuoteFormatsCents(t *testing.T) {
	var b bytes.Buffer
	printQuote(&b, &quotesv1.Quote{Symbol: "ACME", Sequence: 3, PriceMinor: 10205, At: timestamppb.New(time.Date(2026, 11, 24, 9, 5, 7, 42e6, time.UTC))})
	if got := strings.TrimSpace(b.String()); got != "ACME #3 102.05 09:05:07.042" {
		t.Fatalf("%q", got)
	}
}

func TestRunUsageAndUnknownCommand(t *testing.T) {
	var b bytes.Buffer
	if err := run(&b, "127.0.0.1:1", true, time.Second, "t", nil); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Errorf("no args: %v", err)
	}
	if err := run(&b, "127.0.0.1:1", true, time.Second, "t", []string{"explode"}); err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("unknown: %v", err)
	}
	if err := run(&b, "127.0.0.1:1", true, time.Second, "t", []string{"latest"}); err == nil {
		t.Error("latest with no symbol accepted")
	}
}
