package gorqlite_test

import (
	"context"
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
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 1, 'Romulan', 123.456, 0, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 2, 'Vulcan', 123.456, 0, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 3, 'Klingon', 123.456, 1, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 4, 'Ferengi', 123.456, 0, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( 5, 'Cardassian', 123.456, 1, '{\"met\":\""+met+"\"}', "+met+" )")
	wResults, err := globalConnection.Write(s)
	if err != nil {
		t.Errorf("failed during insert: %v", err.Error())
		for _, wr := range wResults {
			if wr.Err != nil {
				t.Errorf("caught error: %v", wr.Err.Error())
			}
		}
	}

	t.Run("Normal case", func(t *testing.T) {
		qr, err := globalConnection.QueryOne("SELECT name, ts, wallet, bankrupt, payload FROM " + testTableName() + "_full WHERE id > 3")
		if err != nil {
			t.Errorf("failed during query: %v", err.Error())
		}

		if qr.NumRows() != 2 {
			t.Errorf("expected 2 row, got %v", qr.NumRows())
		}

		if len(qr.Columns()) != 5 {
			t.Errorf("expected 5 columns, got %v", len(qr.Columns()))
		}

		if len(qr.Columns()) == 5 {
			expect := []string{"name", "ts", "wallet", "bankrupt", "payload"}
			for i, c := range qr.Columns() {
				if c != expect[i] {
					t.Errorf("expected column %v to be %v, got %v", i, expect[i], c)
				}
			}
		}

		if len(qr.Types()) != 5 {
			t.Errorf("expected 5 types, got %v", len(qr.Types()))
		}

		na := qr.Next()
		if na != true {
			t.Errorf("expected true, got %v", na)
		}

		if qr.RowNumber() != 0 {
			t.Errorf("expected row number to be 0, got %v", qr.RowNumber())
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
		var wallet float64
		var bankrupt bool
		var payload []byte
		err = qr.Scan(&id, &name)
		if err == nil {
			t.Errorf("expected an error to be returned, got nil")
		}

		err = qr.Scan(&name, &ts, &wallet, &bankrupt, &payload)
		if err != nil {
			t.Errorf("scanning: %v", err.Error())
		}
		if name != "Ferengi" {
			t.Errorf("name should be 'Ferengi' but it's '%s'", name)
		}

		qr.Next()

		if qr.RowNumber() != 1 {
			t.Errorf("expected row number to be 1, got %v", qr.RowNumber())
		}

		err = qr.Scan(&name, &ts, &wallet, &bankrupt, &payload)
		if err != nil {
			t.Errorf("scanning: %s", err.Error())
		}
		if name != "Cardassian" {
			t.Errorf("expected name to be 'Cardassian', got: %s", name)
		}

		if ts != meeting {
			t.Errorf("expected ts to equal meeting, got ts: %v, meeting: %v", ts, meeting)
		}

		if qr.Next() == true {
			t.Errorf("expected no more rows, got one")
		}
	})

	t.Run("Invalid Query", func(t *testing.T) {
		qr, err := globalConnection.QueryOne("INVALID QUERY")
		if err == nil {
			t.Errorf("expected an error to be returned, got nil")
		}

		if qr.Err == nil {
			t.Errorf("expected an error to be returned, got nil")
		}
	})

	t.Run("Map before next", func(t *testing.T) {
		qr, err := globalConnection.QueryOne("SELECT name  FROM " + testTableName() + "_full WHERE id = 3")
		if err != nil {
			t.Errorf("failed during query: %v - %v", err.Error(), qr.Err.Error())
		}

		_, err = qr.Map()
		if err == nil {
			t.Errorf("expected an error to be returned, got nil")
		}
	})

	t.Run("Scan before next", func(t *testing.T) {
		qr, err := globalConnection.QueryOne("SELECT name FROM " + testTableName() + "_full WHERE id = 3")
		if err != nil {
			t.Errorf("failed during query: %v - %v", err.Error(), qr.Err.Error())
		}

		var name string
		err = qr.Scan(&name)
		if err == nil {
			t.Errorf("expected an error to be returned, got nil")
		}
	})
}

func TestQueryOneContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	now := time.Now()

	wResults, err := globalConnection.WriteOneContext(
		ctx,
		"INSERT INTO "+testTableName()+"_full (id, name, wallet, bankrupt, payload, ts) VALUES ( ?, ?, ?, ?, ?, ? )",
		11,
		"Vulcan",
		889.3332,
		false,
		[]byte("Lorem ipsum dolor sit amet"),
		now,
	)
	if err != nil {
		t.Errorf("failed during insert: %v - %v", err.Error(), wResults.Err.Error())
	}

	t.Logf("trying QueryOne")
	qr, err := globalConnection.QueryOneContext(ctx, "SELECT id, name, wallet, bankrupt, payload, ts FROM "+testTableName()+"_full WHERE id = ?", 11)
	if err != nil {
		t.Errorf("failed during query: %v - %v", err.Error(), qr.Err.Error())
	}

	na := qr.Next()
	if na != true {
		t.Errorf("expected true, got %v", na)
	}

	var id int64
	var name string
	var ts time.Time
	var wallet float64
	var bankrupt bool
	var payload []byte
	err = qr.Scan(&id, &name, &wallet, &bankrupt, &payload, &ts)
	if err != nil {
		t.Errorf("scanning: %v", err.Error())
	}

	if id != 11 {
		t.Errorf("expected id to be 11, got %v", id)
	}

	if name != "Vulcan" {
		t.Errorf("expected name to be 'Vulcan', got: %s", name)
	}

	if wallet != 889.3332 {
		t.Errorf("expected wallet to be 889.3332, got: %v", wallet)
	}

	if bankrupt != false {
		t.Errorf("expected bankrupt to be false, got: %v", bankrupt)
	}

	if len(payload) == 0 {
		t.Errorf("expected payload to be non-empty, got: %v", payload)
	}

	if !ts.Equal(now) {
		t.Errorf("expected ts to equal now, got ts: %v, now: %v", ts, now)
	}
}
