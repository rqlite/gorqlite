package gorqlite_test

import (
	"fmt"
	"testing"
	"time"
)

func TestQueryOne(t *testing.T) {
	// give an extra second for time diff between local and rqlite
	started := time.Now().Add(-time.Second)

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	t.Logf("trying Write INSERT")
	s := make([]string, 0)
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 1, 'Romulan', 20000, 0, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 2, 'Vulcan', 20000, 0, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 3, 'Klingon', 20000, 1, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 4, 'Ferengi', 20000, 0, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 5, 'Cardassian', 25000, 1, '{\"met\":\""+met+"\"}', "+met+" )")
	wResults, err := globalConnection.Write(s)
	if err != nil {
		t.Errorf("failed during insert: %v", err.Error())
		for _, wr := range wResults {
			if wr.Err != nil {
				t.Errorf("caught error: %v", wr.Err.Error())
			}
		}
	}

	t.Logf("trying QueryOne")
	qr, err := globalConnection.QueryOne("SELECT name, ts FROM " + testTableName() + "_full WHERE id > 3")
	if err != nil {
		t.Errorf("failed during query: %v", err.Error())
	}

	na := qr.Next()
	if na != true {
		t.Errorf("expected true, got %v", na)
	}

	t.Logf("trying Map()")
	r, err := qr.Map()
	if err != nil {
		t.Errorf("failed during map: %v", err.Error())
	}

	if r["name"].(string) != "Ferengi" {
		t.Errorf("expected Ferengi, got %v", r["name"])
	}
	if ts, ok := r["ts"]; ok {
		if ts, ok := ts.(time.Time); ok {
			// time should not be zero because it defaults to current utc time
			if ts.IsZero() {
				t.Errorf("time should not be zero, got zero")
			} else if ts.Before(started) {
				t.Errorf("time %q is before start %q", ts, started)
			}
		} else {
			t.Errorf("ts is a real %T", ts)
		}
	} else {
		t.Logf("--> FAILED: ts not found")
	}

	t.Logf("trying Scan(), also float64->int64 in Scan()")
	var id int64
	var name string
	var ts time.Time
	err = qr.Scan(&id, &name)
	if err == nil {
		t.Errorf("expected an error to be returned, got nil")
	}

	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Errorf("scanning: %v", err.Error())
	}
	if name != "Ferengi" {
		t.Errorf("name should be 'Ferengi' but it's '%s'", name)
	}

	qr.Next()
	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Errorf("scanning: %s", err.Error())
	}
	if name != "Cardassian" {
		t.Errorf("expected name to be 'Cardassian', got: %s", name)
	}

	if ts != meeting {
		t.Errorf("expected ts to equal meeting, got ts: %v, meeting: %v", ts, meeting)
	}
}
