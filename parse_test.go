package gorqlite

import (
	"encoding/json"
	"testing"
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
			// Must not panic.
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
		// Must not panic.
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
