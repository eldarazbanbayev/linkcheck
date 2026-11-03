package checker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// collect drains the channel and indexes results by URL.
func collect(ch <-chan Result) map[string]Result {
	out := make(map[string]Result)
	for r := range ch {
		out[r.URL] = r
	}
	return out
}

func TestCheckReportsStatuses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/ok", http.StatusFound)
		case "/missing":
			w.WriteHeader(http.StatusNotFound)
		case "/broken":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	c := New(Config{Workers: 3, Timeout: 2 * time.Second})
	got := collect(c.Check(context.Background(), []string{
		srv.URL + "/ok",
		srv.URL + "/redirect",
		srv.URL + "/missing",
		srv.URL + "/broken",
	}))

	want := map[string]int{
		"/ok":       http.StatusOK,
		"/redirect": http.StatusOK, // redirects are followed
		"/missing":  http.StatusNotFound,
		"/broken":   http.StatusInternalServerError,
	}
	for path, status := range want {
		res, ok := got[srv.URL+path]
		if !ok {
			t.Fatalf("no result for %s", path)
		}
		if res.Status != status {
			t.Errorf("%s: status = %d, want %d", path, res.Status, status)
		}
	}
	if !got[srv.URL+"/ok"].OK() {
		t.Error("/ok should be OK")
	}
	if got[srv.URL+"/missing"].OK() {
		t.Error("/missing should not be OK")
	}
}

func TestCheckRetriesTransientFailures(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(Config{Workers: 1, Retries: 2, Backoff: time.Millisecond, Timeout: time.Second})
	res := collect(c.Check(context.Background(), []string{srv.URL}))[srv.URL]

	if !res.OK() {
		t.Fatalf("result = %+v, want OK after retries", res)
	}
	if res.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", res.Attempts)
	}
}

func TestCheckCapsConcurrency(t *testing.T) {
	const workers = 3
	var inFlight, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	urls := make([]string, 12)
	for i := range urls {
		urls[i] = srv.URL + "/item/" + strconv.Itoa(i)
	}
	c := New(Config{Workers: workers, Timeout: 2 * time.Second})
	got := collect(c.Check(context.Background(), urls))

	if len(got) != len(urls) {
		t.Fatalf("got %d results, want %d", len(got), len(urls))
	}
	if p := peak.Load(); p > workers || p < 2 {
		t.Fatalf("peak concurrency = %d, want between 2 and %d", p, workers)
	}
}

func TestCheckStopsOnCancel(t *testing.T) {
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done() // hang until the client gives up
	}))
	defer srv.Close()

	urls := make([]string, 50)
	for i := range urls {
		urls[i] = srv.URL + "/slow/" + strconv.Itoa(i)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := New(Config{Workers: 4, Timeout: 10 * time.Second}).Check(ctx, urls)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("no request reached the server")
	}
	cancel()

	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("checker did not stop after cancel")
	}
}

func TestCheckTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(300 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	c := New(Config{Workers: 1, Timeout: 50 * time.Millisecond})
	res := collect(c.Check(context.Background(), []string{srv.URL}))[srv.URL]
	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", res.Err)
	}
}

func TestCheckRejectsBadURLsWithoutRequests(t *testing.T) {
	c := New(Config{Workers: 2, Retries: 3})
	got := collect(c.Check(context.Background(), []string{"not a url", "ftp://example.com/file"}))

	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}
	for u, res := range got {
		if res.Err == nil || res.Attempts != 0 {
			t.Errorf("%q: err=%v attempts=%d, want an error and 0 attempts", u, res.Err, res.Attempts)
		}
	}
}
