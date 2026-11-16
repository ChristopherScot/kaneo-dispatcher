package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestKaneoRateLimitRecovery(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	d := dispatcher{kaneoURL: srv.URL, client: srv.Client()}
	start := time.Now()
	if err := d.kaneo(t.Context(), "/task/a", nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || time.Since(start) < time.Second {
		t.Fatalf("did not honor GET backoff: calls=%d elapsed=%v", calls, time.Since(start))
	}
}

func TestKaneoCooldownIsSharedAndWritesAreNotReplayed(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	d := dispatcher{kaneoURL: srv.URL, client: srv.Client()}
	err := d.kaneoRequest(t.Context(), http.MethodPut, "/task/a", map[string]string{"status": "to-do"}, nil)
	var status *httpStatusError
	if !errors.As(err, &status) || status.Code != 429 || calls != 1 {
		t.Fatalf("PUT replayed or rejection lost: calls=%d err=%v", calls, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := d.kaneo(ctx, "/task/b", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read did not honor cancellation during shared cooldown: %v", err)
	}
	if calls != 1 {
		t.Fatalf("another request bypassed the shared cooldown: calls=%d", calls)
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 16, 4, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{
		{"12", 12 * time.Second},
		{now.Add(45 * time.Second).Format(http.TimeFormat), 45 * time.Second},
		{"", time.Minute}, {"invalid", time.Minute}, {"-1", time.Minute},
		{"0", time.Second}, {now.Add(-time.Minute).Format(http.TimeFormat), time.Second},
	} {
		if got := retryAfter(tc.header, now).Sub(now); got != tc.want {
			t.Errorf("Retry-After %q: %v, want %v", tc.header, got, tc.want)
		}
	}
}
