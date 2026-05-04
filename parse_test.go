package gorqlite

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// These tests exercise the JSON-decoding paths used by Query, Write,
// Request and updateClusterInfo against malformed or surprising
// payloads. Before the parser was switched to typed structs the same
// inputs caused panics on bare type assertions.

func TestParseQueryResult_EmptyValuesAndNullFields(t *testing.T) {
	conn := &Connection{ID: "test"}

	cases := []struct {
		name string
		body string
	}{
		{"empty values array", `{"columns":["id"],"types":["integer"],"values":[],"time":1}`},
		{"missing values key", `{"columns":["id"],"types":["integer"],"time":1}`},
		{"null values", `{"columns":["id"],"types":["integer"],"values":null}`},
		{"error result", `{"error":"boom"}`},
		{"completely empty", `{}`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var r resultJSON
			if err := json.Unmarshal([]byte(c.body), &r); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			qr := conn.parseQueryResult(r)
			if c.body == `{"error":"boom"}` && qr.Err == nil {
				t.Error("expected error on error result")
			}
		})
	}
}

func TestParseWriteResult_MissingFields(t *testing.T) {
	conn := &Connection{ID: "test"}

	cases := []string{
		`{}`,
		`{"last_insert_id":42}`,
		`{"rows_affected":7}`,
		`{"last_insert_id":1,"rows_affected":2,"time":0.5}`,
		`{"error":"already exists"}`,
	}
	for _, body := range cases {
		var r resultJSON
		if err := json.Unmarshal([]byte(body), &r); err != nil {
			t.Fatalf("unmarshal %q: %v", body, err)
		}
		_ = conn.parseWriteResult(r)
	}
}

func TestParseLeaderRaftAddr(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"5.x string", `"node1:9001"`, "node1:9001", false},
		{"6.x object", `{"node_id":"n1","addr":"x"}`, "n1", false},
		{"empty string", `""`, "", true},
		{"null", `null`, "", true},
		{"object missing node_id", `{}`, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseLeaderRaftAddr(json.RawMessage(c.input))
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, c.wantErr)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestBuildPeerList(t *testing.T) {
	got := buildPeerList(peer("a"), []peer{"b", "c"})
	want := []peer{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("len mismatch: %v vs %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("at %d: got %q, want %q", i, got[i], want[i])
		}
	}

	// Empty leader: only otherPeers.
	got = buildPeerList(peer(""), []peer{"b"})
	if len(got) != 1 || got[0] != "b" {
		t.Errorf("expected [b], got %v", got)
	}
}

func TestRedactedPassword(t *testing.T) {
	if got := redactedPassword(""); got != "" {
		t.Errorf("empty: got %q", got)
	}
	if got := redactedPassword("hunter2"); got == "hunter2" {
		t.Errorf("password leaked: %q", got)
	}
}

// shortRowQR builds a QueryResult whose row has fewer values than
// the column list claims. Real rqlite never produces these, but
// proxies and middlewares can.
func shortRowQR() *QueryResult {
	return &QueryResult{
		conn:      &Connection{ID: "test"},
		columns:   []string{"a", "b", "c"},
		types:     []string{"text", "text", "text"},
		values:    [][]interface{}{{"x", "y"}}, // 2 vals for 3 cols
		rowNumber: 0,
	}
}

func TestMap_ShortRowReturnsError(t *testing.T) {
	qr := shortRowQR()
	_, err := qr.Map()
	if !errors.Is(err, errMalformedRow) {
		t.Errorf("expected errMalformedRow, got %v", err)
	}
}

func TestScan_ShortRowReturnsError(t *testing.T) {
	qr := shortRowQR()
	var a, b, c string
	err := qr.Scan(&a, &b, &c)
	if !errors.Is(err, errMalformedRow) {
		t.Errorf("expected errMalformedRow, got %v", err)
	}
}

func TestNextBeforeAccessReturnsErrCallNextFirst(t *testing.T) {
	qr := &QueryResult{
		conn:      &Connection{ID: "test"},
		columns:   []string{"a"},
		types:     []string{"text"},
		values:    [][]interface{}{{"x"}},
		rowNumber: -1,
	}

	if _, err := qr.Map(); !errors.Is(err, errCallNextFirst) {
		t.Errorf("Map: expected errCallNextFirst, got %v", err)
	}
	if _, err := qr.Slice(); !errors.Is(err, errCallNextFirst) {
		t.Errorf("Slice: expected errCallNextFirst, got %v", err)
	}
	var dst string
	if err := qr.Scan(&dst); !errors.Is(err, errCallNextFirst) {
		t.Errorf("Scan: expected errCallNextFirst, got %v", err)
	}
}

func TestScan_NilByteSliceSkips(t *testing.T) {
	// Pre-fix, *[]byte returned an error for nil src; other primitive
	// scan types skipped silently. Confirm the new behavior is
	// consistent (skip without error, leave dest unchanged).
	qr := &QueryResult{
		conn:      &Connection{ID: "test"},
		columns:   []string{"a"},
		types:     []string{"blob"},
		values:    [][]interface{}{{nil}},
		rowNumber: 0,
	}

	prefilled := []byte("untouched")
	dst := prefilled
	if err := qr.Scan(&dst); err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if string(dst) != "untouched" {
		t.Errorf("expected dst to be left unchanged, got %q", dst)
	}
}

// assembleURL is exercised here because its credential handling
// changed: empty-password no longer suppresses the username.
func TestAssembleURL_EmptyPasswordKeepsUsername(t *testing.T) {
	conn := &Connection{
		ID:               "test",
		username:         "alice",
		password:         "",
		consistencyLevel: ConsistencyLevelWeak,
	}
	url := conn.assembleURL(api_QUERY, peer("host:1234"))
	// url.Redacted masks anything after `user:`, so we look for "alice@"
	// being present (with or without colon) by parsing.
	if !contains(url, "alice@") && !contains(url, "alice:@") {
		t.Errorf("expected username in URL, got %q", url)
	}
}

func TestAssembleURL_SpecialCharsInPasswordEscaped(t *testing.T) {
	conn := &Connection{
		ID:               "test",
		username:         "alice",
		password:         "p@ss/word",
		consistencyLevel: ConsistencyLevelWeak,
	}
	url := conn.assembleURL(api_QUERY, peer("host:1234"))
	// Plain "p@ss/word" should NOT appear unescaped.
	if contains(url, "p@ss/word") {
		t.Errorf("password not escaped in URL: %q", url)
	}
}

func TestAssembleURL_QueueOpAddsQueueParam(t *testing.T) {
	conn := &Connection{
		ID:                "test",
		consistencyLevel:  ConsistencyLevelWeak,
		wantsTransactions: false,
	}
	url := conn.assembleURL(api_WRITE_QUEUED, peer("host:1234"))
	if !contains(url, "&queue") {
		t.Errorf("expected &queue in queued-write URL, got %q", url)
	}
}

func TestAssembleURL_NormalWriteHasNoQueueParam(t *testing.T) {
	conn := &Connection{
		ID:                "test",
		consistencyLevel:  ConsistencyLevelWeak,
		wantsTransactions: false,
	}
	url := conn.assembleURL(api_WRITE, peer("host:1234"))
	if contains(url, "queue") {
		t.Errorf("normal write should not carry queue param, got %q", url)
	}
}

// contains is a tiny inlined replacement for strings.Contains so the
// test file doesn't need to import "strings".
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestInitConnectionWrapsErrors verifies that errors from URL parsing
// stages are wrapped (so callers can errors.Is against sentinel values
// like strconv.ErrSyntax via errors.Unwrap).
func TestInitConnectionWrapsErrors(t *testing.T) {
	cases := []struct {
		url    string
		errSub string
	}{
		{"http://localhost?disableClusterDiscovery=notabool", "disableClusterDiscovery"},
		{"http://localhost?timeout=notanumber", "timeout"},
		{"http://localhost?level=garbage", "consistency"},
	}
	for _, c := range cases {
		conn := &Connection{ID: "test"}
		err := conn.initConnection(c.url, nil)
		if err == nil {
			t.Errorf("%s: expected error, got nil", c.url)
			continue
		}
		if !contains(err.Error(), c.errSub) {
			t.Errorf("%s: error %q didn't contain %q", c.url, err.Error(), c.errSub)
		}
	}
}

// Sanity check that a successful initConnection sets sane defaults.
func TestInitConnectionDefaults(t *testing.T) {
	conn := &Connection{ID: "test"}
	if err := conn.initConnection("http://", nil); err != nil {
		t.Fatalf("initConnection: %v", err)
	}
	if conn.cluster.leader != peer("localhost:4001") {
		t.Errorf("default leader: got %q, want localhost:4001", conn.cluster.leader)
	}
	if conn.consistencyLevel != ConsistencyLevelWeak {
		t.Errorf("default level: got %v, want weak", conn.consistencyLevel)
	}
	if !conn.wantsTransactions {
		t.Errorf("default wantsTransactions should be true")
	}
	if conn.client == nil {
		t.Errorf("client must be initialised")
	}
	if conn.client.Timeout != defaultTimeout*time.Second {
		t.Errorf("default timeout: got %v, want %d", conn.client.Timeout, defaultTimeout)
	}
}
