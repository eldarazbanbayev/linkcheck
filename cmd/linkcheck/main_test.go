package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadURLsSkipsBlanksAndComments(t *testing.T) {
	in := strings.NewReader("https://a.example\n\n# skip me\n  https://b.example  \n")
	got, err := readURLs(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "https://a.example" || got[1] != "https://b.example" {
		t.Fatalf("got %q", got)
	}
}

func TestRunReportsFailuresWithExitCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bad" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var out, errOut bytes.Buffer
	code := run([]string{"-gap=0", "-retries=0", srv.URL + "/good", srv.URL + "/bad"},
		strings.NewReader(""), &out, &errOut)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(out.String(), "2 checked: 1 ok, 1 failed") {
		t.Fatalf("unexpected output:\n%s", out.String())
	}
}
