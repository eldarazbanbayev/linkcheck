// Command linkcheck probes URLs concurrently and reports their status.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/eldarazbanbayev/linkcheck/internal/checker"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// run holds the program logic so tests can drive it. It returns the process exit code.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("linkcheck", flag.ContinueOnError)
	fs.SetOutput(stderr)
	workers := fs.Int("c", 8, "concurrent requests")
	timeout := fs.Duration("timeout", 5*time.Second, "timeout per attempt")
	retries := fs.Int("retries", 2, "extra attempts on 429, 5xx and network errors")
	backoff := fs.Duration("backoff", 300*time.Millisecond, "base delay between attempts (grows linearly)")
	gap := fs.Duration("gap", 200*time.Millisecond, "minimum delay between requests to one host")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	urls, err := readURLs(fs.Args(), stdin)
	if err != nil {
		fmt.Fprintln(stderr, "reading input:", err)
		return 2
	}
	if len(urls) == 0 {
		fmt.Fprintln(stderr, "usage: linkcheck [flags] URL... (or pipe URLs on stdin)")
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c := checker.New(checker.Config{
		Workers: *workers,
		Timeout: *timeout,
		Retries: *retries,
		Backoff: *backoff,
		HostGap: *gap,
	})

	var ok, failed int
	for res := range c.Check(ctx, urls) {
		if res.OK() {
			ok++
		} else {
			failed++
		}
		fmt.Fprintln(stdout, formatResult(res))
	}
	fmt.Fprintf(stdout, "\n%d checked: %d ok, %d failed\n", ok+failed, ok, failed)

	if failed > 0 || ctx.Err() != nil {
		return 1
	}
	return 0
}

// readURLs uses positional args when given, otherwise reads one URL per line from stdin.
func readURLs(args []string, stdin io.Reader) ([]string, error) {
	if len(args) > 0 {
		return args, nil
	}
	var urls []string
	sc := bufio.NewScanner(stdin)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		urls = append(urls, line)
	}
	return urls, sc.Err()
}

func formatResult(r checker.Result) string {
	status := "FAIL"
	if r.OK() {
		status = "OK"
	}
	code := "-"
	if r.Status != 0 {
		code = strconv.Itoa(r.Status)
	}
	line := fmt.Sprintf("%-4s %4s %8s  %s", status, code, r.Latency.Round(time.Millisecond), r.URL)
	if r.Err != nil {
		line += "  (" + r.Err.Error() + ")"
	}
	return line
}
