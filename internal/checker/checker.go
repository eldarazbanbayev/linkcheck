// Package checker probes many URLs concurrently: a bounded worker pool, spacing per host,
// per-attempt timeouts and retries on transient failures.
package checker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const maxDrainBytes = 64 << 10 // read at most 64 KiB so connections can be reused

var errBadURL = errors.New("not an absolute http(s) URL")

// Config controls a Checker. Zero values fall back to defaults.
type Config struct {
	Workers int           // concurrent requests in flight (default 4)
	Timeout time.Duration // timeout per attempt (default 5s)
	Retries int           // extra attempts on 429, 5xx and network errors
	Backoff time.Duration // base delay; attempt n waits n*Backoff before the next try
	HostGap time.Duration // minimum delay between requests to the same host
	Client  *http.Client  // optional; defaults to a plain client
}

// Result is the final outcome for one URL.
type Result struct {
	URL      string
	Status   int // 0 when no HTTP response was received
	Latency  time.Duration
	Attempts int
	Err      error
}

// OK reports whether the URL ended with a 2xx or 3xx status and no error.
func (r Result) OK() bool {
	return r.Err == nil && r.Status >= 200 && r.Status < 400
}

// Checker runs URL checks. A Checker can be reused across Check calls.
type Checker struct {
	cfg     Config
	client  *http.Client
	limiter *HostLimiter
}

// New builds a Checker, applying defaults to unset fields.
func New(cfg Config) *Checker {
	if cfg.Workers < 1 {
		cfg.Workers = 4
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Retries < 0 {
		cfg.Retries = 0
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{}
	}
	return &Checker{cfg: cfg, client: client, limiter: NewHostLimiter(cfg.HostGap)}
}

// Check probes every URL and streams results in completion order.
// The channel closes when all URLs are done or ctx is cancelled. Callers must keep
// receiving from it (or cancel ctx), otherwise workers stay blocked.
func (c *Checker) Check(ctx context.Context, urls []string) <-chan Result {
	jobs := make(chan string)
	results := make(chan Result)

	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				res := c.checkOne(ctx, u)
				select {
				case results <- res:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, u := range urls {
			select {
			case jobs <- u:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

// checkOne runs the attempt loop for a single URL.
func (c *Checker) checkOne(ctx context.Context, rawURL string) Result {
	res := Result{URL: rawURL}
	host, err := parseHost(rawURL)
	if err != nil {
		res.Err = err
		return res // permanent failure: no request, no retries
	}

	for attempt := 1; attempt <= c.cfg.Retries+1; attempt++ {
		res.Attempts = attempt

		if err := c.limiter.Wait(ctx, host); err != nil {
			res.Err = err
			return res
		}

		start := time.Now()
		status, err := c.do(ctx, rawURL)
		res.Status, res.Err, res.Latency = status, err, time.Since(start)

		transient := err != nil || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
		if !transient || attempt > c.cfg.Retries || ctx.Err() != nil {
			return res
		}
		if !sleep(ctx, time.Duration(attempt)*c.cfg.Backoff) {
			res.Err = ctx.Err()
			return res
		}
	}
	return res
}

// do performs one HTTP GET attempt and returns the response status.
func (c *Checker) do(ctx context.Context, rawURL string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))
	return resp.StatusCode, nil
}

// parseHost accepts only absolute http(s) URLs and returns the host used for rate limiting.
func parseHost(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", errBadURL
	}
	return u.Host, nil
}

// sleep pauses for d unless ctx ends first. It reports whether the full pause elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
