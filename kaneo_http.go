package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// The webhook and reconciler share one API key. A 429 cools down both, and
// changes the generation so validation passes can discard pre-wait facts.
type kaneoCooldown struct {
	mu         sync.Mutex
	until      time.Time
	generation uint64
}

func (c *kaneoCooldown) state() (time.Time, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.until, c.generation
}

func (c *kaneoCooldown) observe(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if at.After(c.until) {
		c.until = at
	}
	c.generation++
}

// A local deferral proves no HTTP mutation was sent. Unlike a server error,
// callers may release their own write claim and retry from fresh observations.
type kaneoDeferredError struct{ until time.Time }

func (e *kaneoDeferredError) Error() string {
	return fmt.Sprintf("Kaneo request deferred until %s", e.until.UTC().Format(time.RFC3339))
}

var errKaneoObservationChanged = errors.New("Kaneo validation crossed a rate-limit wait; retry with fresh observations")

func (d *dispatcher) kaneoRequest(ctx context.Context, method, path string, body, out any) error {
	// Bound detached webhook reads as well as retries. Caller deadlines still
	// take precedence. Mutations are attempted at most once, never replayed.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for attempt := 0; ; attempt++ {
		for {
			until, _ := d.kaneoLimit.state()
			if delay := time.Until(until); delay > 0 {
				if method != http.MethodGet {
					return &kaneoDeferredError{until: until}
				}
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
				continue // another request may have extended the cooldown
			}
			break
		}
		err := requestJSON(ctx, d.client, method, d.kaneoURL+"/api"+path, d.apiKey, body, out)
		var status *httpStatusError
		if !errors.As(err, &status) || status.Code != http.StatusTooManyRequests {
			return err
		}
		d.kaneoLimit.observe(status.RetryAt)
		if method != http.MethodGet || attempt == 2 {
			return err
		}
	}
}
