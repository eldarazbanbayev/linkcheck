# linkcheck

Concurrent URL health checker written with the Go standard library only. Requires Go 1.22+.

- bounded worker pool (`-c`); results stream out as they finish
- per-host spacing (`-gap`), so a single domain is never hammered
- per-attempt timeouts; retries with linear backoff on 429, 5xx and network errors
- Ctrl+C cancels in-flight work; cancellation is covered by tests and leaks no goroutines
- exit code 1 if any URL failed, so it works in CI

## Usage

```sh
make build
./bin/linkcheck https://go.dev https://example.com/does-not-exist
cat urls.txt | ./bin/linkcheck -c 16 -timeout 3s -gap 100ms
```

When reading from stdin, blank lines and lines starting with `#` are ignored.

## Layout

```
cmd/linkcheck                CLI: flags, input, output, exit codes
internal/checker/checker.go  worker pool, attempts, retries, timeouts
internal/checker/limiter.go  per-host spacing via reserved time slots
```

## Test

```sh
make test   # go test -race -count=1 ./...
```

Tests run against `httptest` servers and cover status mapping, retries, the concurrency cap, cancellation, timeouts and bad input.

## Design notes

- Workers never block after cancellation: every send selects on `ctx.Done()`.
- `HostLimiter` reserves time slots under a mutex instead of keeping a goroutine per host.
- A production service would more likely use `golang.org/x/time/rate`. The limiter is hand-written here to keep the project dependency-free.
