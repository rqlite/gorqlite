package gorqlite_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rqlite/gorqlite"
)

// statusJSON is enough /status payload to satisfy updateClusterInfo
// when bringing up a fake server in these tests.
const statusJSON = `{"store":{"leader":"127.0.0.1:9001","metadata":{"127.0.0.1:9001":{"api_addr":"%s"}}}}`

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
		// Strip the http:// from URL so we have just host:port.
		host := strings.TrimPrefix(srv.URL, "http://")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"store\":{\"leader\":\"" + host + "\",\"metadata\":{\"" + host + "\":{\"api_addr\":\"http://" + host + "\"}}}}"))
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

			// QueryOne must never panic regardless of payload.
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

	var seen sync.Map
	srv := newFakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.URL.RawQuery, true)
		// Always respond as if it were a queued write so the call
		// succeeds for QueueOne and WriteOne both wouldn't matter
		// here — we only care about no race + no panic.
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
