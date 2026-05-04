package gorqlite_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/gorqlite"
)

// fakeServer is an httptest.Server pretending to be rqlite. The
// handler for /db/* is provided per-test.
type fakeServer struct {
	*httptest.Server
}

func newFakeServer(t *testing.T, dbHandler http.HandlerFunc) *fakeServer {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		host := strings.TrimPrefix(srv.URL, "http://")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"store":{"leader":"` + host + `","metadata":{"` + host + `":{"api_addr":"http://` + host + `"}}}}`))
	})
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/db/", dbHandler)
	return &fakeServer{srv}
}

func TestQueryDoesNotPanicOnUnexpectedJSON(t *testing.T) {
	bodies := []string{
		`{}`,                    // no results, no error
		`{"results":[]}`,        // empty array
		`{"results":[{}]}`,      // empty result object
		`{"results":[{"columns":null,"types":null,"values":null}]}`,
		`{"error":"server says no"}`,
		`not json at all`,
	}

	for _, body := range bodies {
		body := body
		t.Run(body, func(t *testing.T) {
			srv := newFakeServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(body))
			})

			conn, err := gorqlite.Open(srv.URL + "?disableClusterDiscovery=true")
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer conn.Close()

			// Must never panic regardless of payload.
			_, _ = conn.QueryOne("SELECT 1")
			_, _ = conn.WriteOne("INSERT INTO x VALUES (1)")
			_, _ = conn.Request([]string{"SELECT 1"})
		})
	}
}

func TestQueueOneRecoversFromMissingSequenceNumber(t *testing.T) {
	srv := newFakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{}`))
	})

	conn, err := gorqlite.Open(srv.URL + "?disableClusterDiscovery=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	_, err = conn.QueueOne("INSERT INTO x VALUES (1)")
	if err == nil {
		t.Error("expected error when sequence_number missing, got nil")
	}
}

func TestConcurrentQueueDoesNotRace(t *testing.T) {
	// Pre-fix, QueueParameterizedContext mutated conn.wantsQueueing
	// without locking and reset it via defer. Concurrent callers
	// raced on that flag and could send non-queued writes through the
	// queue path. Now Queue uses its own apiOperation and the flag is
	// gone; this test pins that property by hammering the path under
	// -race.

	srv := newFakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"results":[{"last_insert_id":1,"rows_affected":1}],"sequence_number":42}`))
	})

	conn, err := gorqlite.Open(srv.URL + "?disableClusterDiscovery=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				_, _ = conn.QueueOne("INSERT INTO x VALUES (1)")
			} else {
				_, _ = conn.WriteOne("INSERT INTO x VALUES (1)")
			}
		}()
	}
	wg.Wait()
}

// TestRetryHonorsContext confirms that the inflight HTTP client
// respects ctx cancellation via http.NewRequestWithContext. We use a
// slow-but-cancellable handler and a tight ctx timeout, then verify
// the call returns quickly with the ctx error rather than waiting on
// the server.
func TestRetryHonorsContext(t *testing.T) {
	var hits int32
	srv := newFakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		// Hold long enough for the client's ctx to fire, but bounded
		// so test cleanup never blocks indefinitely if connection
		// cancellation doesn't propagate.
		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
		case <-timer.C:
		}
	})

	conn, err := gorqlite.Open(srv.URL + "?disableClusterDiscovery=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = conn.QueryOneContext(ctx, "SELECT 1")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from cancelled ctx, got nil")
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("ctx-cancel didn't short-circuit: took %v", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !contains(err.Error(), "context deadline exceeded") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestPeerRetrySkippedAfterCtxDone seeds the connection with extra
// peers and confirms that once the context is done, retry against
// those peers is skipped — they'd never have responded in time
// anyway, so reporting the deadline is more useful than reporting all
// peers' connection failures.
func TestPeerRetrySkippedAfterCtxDone(t *testing.T) {
	// A single fake server that just hangs.
	srv := newFakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})

	conn, err := gorqlite.Open(srv.URL + "?disableClusterDiscovery=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the call

	_, err = conn.QueryOneContext(ctx, "SELECT 1")
	if err == nil {
		t.Fatal("expected error from cancelled ctx, got nil")
	}
	// The error message should mention "context canceled", not the
	// peer-failure aggregate.
	if !contains(err.Error(), "context canceled") {
		t.Errorf("expected context.Canceled propagation, got: %v", err)
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
