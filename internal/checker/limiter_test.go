package checker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHostLimiterSpacesRequestsPerHost(t *testing.T) {
	const gap = 40 * time.Millisecond
	l := NewHostLimiter(gap)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 3; i++ {
		if err := l.Wait(ctx, "example.com"); err != nil {
			t.Fatal(err)
		}
	}
	// The first call is immediate; the second and third each wait one gap.
	// Timers never fire early, so this lower bound is deterministic.
	if elapsed := time.Since(start); elapsed < 2*gap {
		t.Fatalf("3 calls took %v, want at least %v", elapsed, 2*gap)
	}
}

func TestHostLimiterIsolatesHosts(t *testing.T) {
	l := NewHostLimiter(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, host := range []string{"a.example", "b.example", "c.example"} {
		if err := l.Wait(ctx, host); err != nil {
			t.Fatalf("%s blocked: %v", host, err)
		}
	}
}

func TestHostLimiterHonoursContext(t *testing.T) {
	l := NewHostLimiter(time.Hour)
	ctx := context.Background()
	if err := l.Wait(ctx, "x.example"); err != nil { // first call is immediate
		t.Fatal(err)
	}

	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := l.Wait(short, "x.example"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}
