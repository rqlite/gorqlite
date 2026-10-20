package gorqlite_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/rqlite/gorqlite"
)

// errorV2Body is what rqlite v10.6.0 and later send for a failed
// statement by default: the deprecated error string alongside the
// structured error_v2.
const errorV2Body = `{"results":[{"error":"UNIQUE constraint failed: t.id",` +
	`"error_v2":{"message":"UNIQUE constraint failed: t.id","sqlite":{"code":19,"extended_code":1555}}}]}`

// statementErrorCalls runs each statement-executing method against conn
// and returns the per-result error and the batch error it produced.
func statementErrorCalls(conn *gorqlite.Connection) map[string]func() (error, error) {
	return map[string]func() (error, error){
		"Query": func() (error, error) {
			qr, err := conn.QueryOne("SELECT 1")
			return qr.Err, err
		},
		"Write": func() (error, error) {
			wr, err := conn.WriteOne("INSERT INTO t VALUES (1)")
			return wr.Err, err
		},
		"Request": func() (error, error) {
			results, err := conn.Request([]string{"INSERT INTO t VALUES (1)"})
			if len(results) != 1 {
				return nil, err
			}
			return results[0].Err, err
		},
	}
}

// TestStatementErrorCarriesSQLiteCodes pins that a statement failure is
// surfaced as a *StatementError holding the SQLite result codes from
// error_v2, both on the result and through the batch error, and that
// its message is the server's error text verbatim.
func TestStatementErrorCarriesSQLiteCodes(t *testing.T) {
	srv := newFakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(errorV2Body))
	})

	conn, err := gorqlite.Open(srv.URL + "?disableClusterDiscovery=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	for name, call := range statementErrorCalls(conn) {
		t.Run(name, func(t *testing.T) {
			resultErr, batchErr := call()
			if resultErr == nil || batchErr == nil {
				t.Fatalf("result err=%v, batch err=%v, want both set", resultErr, batchErr)
			}
			if resultErr.Error() != "UNIQUE constraint failed: t.id" {
				t.Errorf("got %q, want the server's message verbatim", resultErr)
			}
			for _, err := range []error{resultErr, batchErr} {
				var se *gorqlite.StatementError
				if !errors.As(err, &se) {
					t.Fatalf("errors.As(%T) did not find a *StatementError", err)
				}
				if se.SQLite == nil {
					t.Fatal("SQLite codes missing")
				}
				if se.SQLite.Code != 19 || se.SQLite.ExtendedCode != 1555 {
					t.Errorf("got code %d/%d, want 19/1555", se.SQLite.Code, se.SQLite.ExtendedCode)
				}
			}
		})
	}
}

// TestStatementErrorFormats pins how the three renderings rqlite can
// send for a failed statement are decoded: the legacy error string on
// its own (pre-v10.6.0, or errors=v1), error_v2 on its own (errors=v2),
// and a non-SQLite failure which has error_v2 but no codes.
func TestStatementErrorFormats(t *testing.T) {
	cases := map[string]struct {
		body     string
		wantMsg  string
		wantCode *gorqlite.SQLiteErrorCodes
	}{
		"legacy error only": {
			body:    `{"results":[{"error":"no such table: nope"}]}`,
			wantMsg: "no such table: nope",
		},
		"error_v2 only": {
			body:     `{"results":[{"error_v2":{"message":"no such table: nope","sqlite":{"code":1,"extended_code":1}}}]}`,
			wantMsg:  "no such table: nope",
			wantCode: &gorqlite.SQLiteErrorCodes{Code: 1, ExtendedCode: 1},
		},
		"error_v2 without sqlite codes": {
			body:    `{"results":[{"error":"context deadline exceeded","error_v2":{"message":"context deadline exceeded"}}]}`,
			wantMsg: "context deadline exceeded",
		},
		"error_v2 with system errno": {
			body:     `{"results":[{"error_v2":{"message":"disk I/O error","sqlite":{"code":10,"extended_code":1034,"system_errno":5}}}]}`,
			wantMsg:  "disk I/O error",
			wantCode: &gorqlite.SQLiteErrorCodes{Code: 10, ExtendedCode: 1034, SystemErrno: 5},
		},
	}

	for name, c := range cases {
		c := c
		t.Run(name, func(t *testing.T) {
			srv := newFakeServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(c.body))
			})
			conn, err := gorqlite.Open(srv.URL + "?disableClusterDiscovery=true")
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer conn.Close()

			for method, call := range statementErrorCalls(conn) {
				t.Run(method, func(t *testing.T) {
					resultErr, batchErr := call()
					if batchErr == nil {
						t.Fatal("batch err is nil, want the statement failure reported")
					}
					var se *gorqlite.StatementError
					if !errors.As(resultErr, &se) {
						t.Fatalf("result err %T is not a *StatementError", resultErr)
					}
					if se.Message != c.wantMsg {
						t.Errorf("message: got %q, want %q", se.Message, c.wantMsg)
					}
					switch {
					case c.wantCode == nil && se.SQLite != nil:
						t.Errorf("got SQLite codes %+v, want none", *se.SQLite)
					case c.wantCode != nil && se.SQLite == nil:
						t.Errorf("got no SQLite codes, want %+v", *c.wantCode)
					case c.wantCode != nil && *se.SQLite != *c.wantCode:
						t.Errorf("got SQLite codes %+v, want %+v", *se.SQLite, *c.wantCode)
					}
				})
			}
		})
	}
}

// TestSuccessfulResultHasNoStatementError pins that a result without
// error or error_v2 is not mistaken for a failure.
func TestSuccessfulResultHasNoStatementError(t *testing.T) {
	srv := newFakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"results":[{"last_insert_id":1,"rows_affected":1}]}`))
	})
	conn, err := gorqlite.Open(srv.URL + "?disableClusterDiscovery=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	wr, err := conn.WriteOne("INSERT INTO t VALUES (1)")
	if err != nil || wr.Err != nil {
		t.Fatalf("err=%v, wr.Err=%v, want both nil", err, wr.Err)
	}
	results, err := conn.Request([]string{"INSERT INTO t VALUES (1)"})
	if err != nil || results[0].Err != nil || results[0].Write == nil {
		t.Fatalf("err=%v, results[0]=%+v, want a write result with no error", err, results[0])
	}
}
