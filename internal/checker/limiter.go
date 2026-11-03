package checker

import (
	"context"
	"sync"
	"time"
)

// HostLimiter enforces a minimum gap between requests to the same host.
// Each caller reserves the next free time slot under a mutex, so no per-host goroutines are needed.
type HostLimiter struct {
	gap  time.Duration
	mu   sync.Mutex
	next map[string]time.Time
}

// NewHostLimiter returns a limiter that spaces requests to each host by at least gap.
func NewHostLimiter(gap time.Duration) *HostLimiter {
	return &HostLimiter{gap: gap, next: make(map[string]time.Time)}
}

// Wait blocks until host may be contacted again, or until ctx is done.
func (l *HostLimiter) Wait(ctx context.Context, host string) error {
	if l.gap <= 0 {
		return ctx.Err()
	}

	l.mu.Lock()
	now := time.Now()
	slot := l.next[host]
	if slot.Before(now) {
		slot = now
	}
	l.next[host] = slot.Add(l.gap)
	l.mu.Unlock()

	delay := slot.Sub(now)
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
