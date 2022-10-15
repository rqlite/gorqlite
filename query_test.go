package gorqlite_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rqlite/gorqlite/v2"
)

func TestQueryOne(t *testing.T) {
	// give an extra second for time diff between local and rqlite
	started := time.Now().Add(-time.Second)

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	t.Logf("trying Write INSERT")
	s := make([]string, 0)
	s = append(s, "CREATE TABLE "+testTableName()+" (id INTEGER, name TEXT, wallet REAL, bankrupt INTEGER, payload BLOB, ts DATETIME)")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name, wallet, bankrupt, payload, ts) VALUES ( 1, 'Romulan', 123.456, 0, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name, wallet, bankrupt, payload, ts) VALUES ( 2, 'Vulcan', 123.456, 0, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name, wallet, bankrupt, payload, ts) VALUES ( 3, 'Klingon', 123.456, 1, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name, wallet, bankrupt, payload, ts) VALUES ( 4, 'Ferengi', 123.456, 0, '{\"met\":\""+met+"\"}', "+fmt.Sprint(time.Now().Unix())+" )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name, wallet, bankrupt, payload, ts) VALUES ( 5, 'Cardassian', 123.456, 1, '{\"met\":\""+met+"\"}', "+met+" )")
	wResults, err := globalConnection.Write(s)
	if err != nil {
		t.Errorf("failed during insert: %v", err.Error())
		for _, wr := range wResults {
			if wr.Err != nil {
				t.Errorf("caught error: %v", wr.Err.Error())
			}
		}
	}

	t.Cleanup(func() {
		results, err := globalConnection.Write([]string{"DROP TABLE " + testTableName()})
		if err != nil {
			t.Errorf("failed during dropping table: %s", err.Error())
			for _, r := range results {
				if r.Err != nil {
					t.Errorf("caught error: %s", err.Error())
				}
			}
		}
	})

	t.Run("Normal case", func(t *testing.T) {
		qr, err := globalConnection.QueryOne("SELECT name, ts, wallet, bankrupt, payload FROM " + testTableName() + " WHERE id > 3")
		if err != nil {
			t.Errorf("failed during query: %v", err.Error())
			if qr.Err != nil {
				t.Errorf("query errors: %s", qr.Err.Error())
			}
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
			t.Errorf("ts not found")
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
		qr, err := globalConnection.QueryOne("SELECT name  FROM " + testTableName() + " WHERE id = 3")
		if err != nil {
			t.Errorf("failed during query: %v - %v", err.Error(), qr.Err.Error())
		}

		_, err = qr.Map()
		if err == nil {
			t.Errorf("expected an error to be returned, got nil")
		}
	})

	t.Run("Scan before next", func(t *testing.T) {
		qr, err := globalConnection.QueryOne("SELECT name FROM " + testTableName() + " WHERE id = 3")
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

	wResults, err := globalConnection.WriteOneContext(
		ctx,
		"CREATE TABLE "+testTableName()+" (id INTEGER, name TEXT, wallet REAL, bankrupt INTEGER, payload BLOB, ts DATETIME)",
	)
	if err != nil {
		t.Fatalf("creating table: %s - %s", err.Error(), wResults.Err.Error())
	}

	t.Cleanup(func() {
		result, err := globalConnection.WriteOne("DROP TABLE " + testTableName())
		if err != nil {
			t.Errorf("dropping table: %s - %s", err.Error(), result.Err.Error())
		}
	})

	now := time.Now().Round(time.Second)

	wResults, err = globalConnection.WriteOneContext(
		ctx,
		fmt.Sprintf(
			"INSERT INTO "+testTableName()+" (id, name, wallet, bankrupt, payload, ts) VALUES ( %d, %q, %v, %t, null, %q )",
			11,
			"Vulcan",
			889.3332,
			false,
			now.Format(time.RFC3339),
		),
	)
	if err != nil {
		t.Errorf("failed during insert: %v - %v", err.Error(), wResults.Err.Error())
	}

	t.Logf("trying QueryOne")
	qr, err := globalConnection.QueryOneContext(ctx, "SELECT id, name, wallet, bankrupt, ts FROM "+testTableName()+" WHERE id = 11")
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
	err = qr.Scan(&id, &name, &wallet, &bankrupt, &ts)
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

	if !ts.Equal(now) {
		t.Errorf("expected ts to equal now, got ts: %v, now: %v", ts, now)
	}
}

func TestQueryOneParameterized(t *testing.T) {
	var qr gorqlite.QueryResult
	var err error

	t.Logf("trying WriteOne DROP")
	_, err = globalConnection.WriteOne("DROP TABLE IF EXISTS " + testTableName())
	if err != nil {
		t.Errorf("dropping table: %s", err.Error())
	}

	// give an extra second for time diff between local and rqlite
	started := time.Now().Add(-time.Second)

	t.Logf("trying WriteOne CREATE")
	_, err = globalConnection.WriteOne("CREATE TABLE " + testTableName() + " (id integer, name text, ts DATETIME DEFAULT CURRENT_TIMESTAMP)")
	if err != nil {
		t.Errorf("craeting new table: %s", err.Error())
	}

	t.Cleanup(func() {
		_, err = globalConnection.WriteOne("DROP TABLE " + testTableName())
		if err != nil {
			t.Errorf("dropping table: %s", err.Error())
		}
	})

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	s := make([]string, 0)
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 1, 'Romulan' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 2, 'Vulcan' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 3, 'Klingon' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 4, 'Ferengi' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name, ts) VALUES ( 5, 'Cardassian',"+met+" )")
	_, err = globalConnection.Write(s)
	if err != nil {
		t.Errorf("inserting bulk queries: %s", err.Error())
	}

	t.Run("QueryOneParameterized", func(t *testing.T) {
		qr, err = globalConnection.QueryOneParameterized(
			gorqlite.ParameterizedStatement{
				Query:     fmt.Sprintf("SELECT name, ts FROM %s WHERE id > ?", testTableName()),
				Arguments: []interface{}{3},
			},
		)
		if err != nil {
			t.Errorf("executing query: %s", err.Error())
		}

		na := qr.Next()
		if na != true {
			t.Error("next should return true, got false")
		}

		r, err := qr.Map()
		if err != nil {
			t.Errorf("map: %s", err.Error())
		}

		if r["name"].(string) != "Ferengi" {
			t.Errorf("expected 'Ferengi', got %s", r["name"].(string))
		}

		if ts, ok := r["ts"]; ok {
			if tss, ok := ts.(time.Time); ok {
				// time should not be zero because it defaults to current utc time
				if tss.IsZero() {
					t.Error("time is zero")
				} else if tss.Before(started) {
					t.Errorf("time %q is before start %q", tss, started)
				}
			} else {
				t.Errorf("ts is a real %T", ts)
			}
		} else {
			t.Error("ts not found")
		}

		t.Logf("trying Scan(), also float64->int64 in Scan()")
		var id int64
		var name string
		var ts time.Time

		err = qr.Scan(&id, &name)
		if err == nil {
			t.Error("expected an error, got nil")
		}

		err = qr.Scan(&name, &ts)
		if err != nil {
			t.Errorf("scanning: %s", err.Error())
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
			t.Errorf("name should be 'Cardassian' but it's '%s'", name)
		}

		if ts != meeting {
			t.Errorf("time should be %q but it's %q", meeting, ts)
		}
	})

}

func TestScanNullableTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	wr, err := globalConnection.WriteOneContext(ctx, "CREATE TABLE "+testTableName()+" (id integer, nullstring text, nullint64 integer, nullint32 integer, nullint16 integer, nullfloat64 real, nullbool integer, nulltime integer) strict")
	if err != nil {
		t.Fatalf("creating table: %s - %s", err.Error(), wr.Err.Error())
	}

	t.Cleanup(func() {
		wr, err := globalConnection.WriteOne("DROP TABLE " + testTableName())
		if err != nil {
			t.Errorf("dropping table: %s - %s", err.Error(), wr.Err.Error())
		}
	})

	var qr gorqlite.QueryResult

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	t.Logf("trying Write INSERT")
	s := make([]string, 0)
	s = append(s, "INSERT INTO "+testTableName()+" (id) VALUES (1)") // other values are gonna be null
	s = append(s, "INSERT INTO "+testTableName()+" (id, nullstring, nullint64, nullint32, nullint16, nullfloat64, nullbool, nulltime) VALUES (2, 'Romulan', 1, 2, 3, 4.5, 1, "+met+")")
	_, err = globalConnection.Write(s)
	if err != nil {
		t.Errorf("insert queries: %s", err.Error())
	}

	t.Logf("trying QueryOne")
	qr, err = globalConnection.QueryOne("SELECT id, nullstring, nullint64, nullint32, nullint16, nullfloat64, nullbool, nulltime FROM " + testTableName() + " WHERE id IN (1, 2)")
	if err != nil {
		t.Errorf("query one: %s", err.Error())
	}

	t.Logf("trying Next()")
	na := qr.Next()
	if na != true {
		t.Error("expected next to be true, got false")
	}

	t.Logf("trying Scan()")
	var id int64
	var nullString gorqlite.NullString
	var nullInt64 gorqlite.NullInt64
	var nullInt32 gorqlite.NullInt32
	var nullInt16 gorqlite.NullInt16
	var nullFloat64 gorqlite.NullFloat64
	var nullBool gorqlite.NullBool
	var nullTime gorqlite.NullTime
	err = qr.Scan(&id, &nullString, &nullInt64, &nullInt32, &nullInt16, &nullFloat64, &nullBool, &nullTime)
	if err != nil {
		t.Errorf("scanning: %s", err.Error())
	}

	if id != 1 {
		t.Errorf("id should be 1 but it's %v", id)
	}

	if nullString.Valid || nullString.String != "" {
		t.Errorf("nullString should be invalid and unset but it's '%v' and '%v'", nullString.Valid, nullString.String)
	}

	if nullInt64.Valid || nullInt64.Int64 != 0 {
		t.Errorf("nullInt64 should be invalid and unset but it's '%v' and '%v'", nullInt64.Valid, nullInt64.Int64)
	}
	if nullInt32.Valid || nullInt32.Int32 != 0 {
		t.Errorf("nullInt32 should be invalid and unset but it's '%v' and '%v'", nullInt32.Valid, nullInt32.Int32)
	}
	if nullInt16.Valid || nullInt16.Int16 != 0 {
		t.Errorf("nullInt16 should be invalid and unset but it's '%v' and '%v'", nullInt16.Valid, nullInt16.Int16)
	}
	if nullFloat64.Valid || nullFloat64.Float64 != 0 {
		t.Errorf("nullFloat64 should be invalid and unset but it's '%v' and '%v'", nullFloat64.Valid, nullFloat64.Float64)
	}
	if nullBool.Valid || nullBool.Bool != false {
		t.Errorf("nullBool should be invalid and unset but it's '%v' and '%v'", nullBool.Valid, nullBool.Bool)
	}
	if nullTime.Valid || !nullTime.Time.IsZero() {
		t.Errorf("nullTime should be invalid and unset but it's '%v' and '%v'", nullTime.Valid, nullTime.Time)
	}

	t.Logf("trying Next()")
	qr.Next()
	if na != true {
		t.Error("expected next to be true, got false")
	}

	t.Logf("trying Scan()")
	err = qr.Scan(&id, &nullString, &nullInt64, &nullInt32, &nullInt16, &nullFloat64, &nullBool, &nullTime)
	if err != nil {
		t.Errorf("scanning: %s", err.Error())
	}
	if id != 2 {
		t.Errorf("id should be 2 but it's %v", id)
	}
	if !nullString.Valid || nullString.String != "Romulan" {
		t.Errorf("nullString should be valid and set to 'Romulan' but it's '%v'", nullString.String)
	}
	if !nullInt64.Valid || nullInt64.Int64 != 1 {
		t.Errorf("nullInt64 should be valid and set to 1 but it's '%v'", nullInt64.Int64)
	}
	if !nullInt32.Valid || nullInt32.Int32 != 2 {
		t.Errorf("nullInt32 should be valid and set to 2 but it's '%v'", nullInt32.Int32)
	}
	if !nullInt16.Valid || nullInt16.Int16 != 3 {
		t.Errorf("nullInt16 should be valid and set to 3 but it's '%v'", nullInt16.Int16)
	}
	if !nullFloat64.Valid || nullFloat64.Float64 != 4.5 {
		t.Errorf("nullFloat64 should be valid and set to 4.5 but it's '%v'", nullFloat64.Float64)
	}
	if !nullBool.Valid || nullBool.Bool != true {
		t.Errorf("nullBool should be valid and set to true but it's '%v'", nullBool.Bool)
	}
	if !nullTime.Valid || !nullTime.Time.Equal(meeting) {
		t.Errorf("nullTime should be valid and set to '%v' but it's '%v'", meeting, nullTime.Time)
	}
}
