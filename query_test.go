package gorqlite

import (
	"fmt"
	"testing"
	"time"
)

func TestQueryOne(t *testing.T) {
	var wr WriteResult
	var qr QueryResult
	var wResults []WriteResult
	var qResults []QueryResult
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	// give an extra second for time diff between local and rqlite
	started := time.Now().Add(-time.Second)

	t.Logf("trying WriteOne CREATE")
	wr, err = conn.WriteOne("CREATE TABLE " + testTableName() + " (id integer, name text, ts DATETIME DEFAULT CURRENT_TIMESTAMP)")
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	t.Logf("trying Write INSERT")
	s := make([]string, 0)
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 1, 'Romulan' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 2, 'Vulcan' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 3, 'Klingon' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 4, 'Ferengi' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name, ts) VALUES ( 5, 'Cardassian',"+met+" )")
	wResults, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying QueryOne")
	qr, err = conn.QueryOne("SELECT name, ts FROM " + testTableName() + " WHERE id > 3")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying Next()")
	na := qr.Next()
	if na != true {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Map()")
	r, err := qr.Map()
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if r["name"].(string) != "Ferengi" {
		t.Logf("--> FAILED, expected 'Ferengi', got %s", r["name"].(string))
		t.Fail()
	}
	if ts, ok := r["ts"]; ok {
		if ts, ok := ts.(time.Time); ok {
			// time should not be zero because it defaults to current utc time
			if ts.IsZero() {
				t.Logf("--> FAILED: time is zero")
				t.Fail()
			} else if ts.Before(started) {
				t.Logf("--> FAILED: time %q is before start %q", ts, started)
				t.Fail()
			}
		} else {
			t.Logf("--> FAILED: ts is a real %T", ts)
			t.Fail()
		}
	} else {
		t.Logf("--> FAILED: ts not found")
	}

	t.Logf("trying Scan()")
	var id int64
	var name string
	var ts time.Time
	err = qr.Scan(&id, &name)
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if name != "Ferengi" {
		t.Logf("--> FAILED, name should be 'Ferengi' but it's '%s'", name)
		t.Fail()
	}
	qr.Next()
	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if name != "Cardassian" {
		t.Logf("--> FAILED, name should be 'Cardassian' but it's '%s'", name)
		t.Fail()
	}
	if ts != meeting {
		t.Logf("--> FAILED, ts should be %q but it's %q", meeting, ts)
		t.Fail()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying Close")
	conn.Close()

	t.Logf("trying WriteOne after Close")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = wr

	t.Logf("trying Write after Close")
	t1 := make([]string, 0)
	t1 = append(t1, "DROP TABLE IF EXISTS "+testTableName()+"")
	t1 = append(t1, "DROP TABLE IF EXISTS "+testTableName()+"")
	wResults, err = conn.Write(t1)
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = wResults

	t.Logf("trying QueryOne after Close")
	qr, err = conn.QueryOne("SELECT id FROM " + testTableName() + "")
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = qr

	t.Logf("trying Query after Close")
	t2 := make([]string, 0)
	t2 = append(t2, "SELECT id FROM "+testTableName()+"")
	t2 = append(t2, "SELECT name FROM "+testTableName()+"")
	t2 = append(t2, "SELECT id,name FROM "+testTableName()+"")
	qResults, err = conn.Query(t2)
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = qResults
}

func TestQueryOneParameterized(t *testing.T) {
	var wr WriteResult
	var qr QueryResult
	var wResults []WriteResult
	var qResults []QueryResult
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	// give an extra second for time diff between local and rqlite
	started := time.Now().Add(-time.Second)

	t.Logf("trying WriteOne CREATE")
	wr, err = conn.WriteOne("CREATE TABLE " + testTableName() + " (id integer, name text, ts DATETIME DEFAULT CURRENT_TIMESTAMP)")
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	t.Logf("trying Write INSERT")
	s := make([]string, 0)
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 1, 'Romulan' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 2, 'Vulcan' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 3, 'Klingon' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 4, 'Ferengi' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name, ts) VALUES ( 5, 'Cardassian',"+met+" )")
	wResults, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying QueryOneParameterized")
	qr, err = conn.QueryOneParameterized(
		ParameterizedStatement{
			Query:     fmt.Sprintf("SELECT name, ts FROM %s WHERE id > ?", testTableName()),
			Arguments: []interface{}{3},
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying Next()")
	na := qr.Next()
	if na != true {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Map()")
	r, err := qr.Map()
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if r["name"].(string) != "Ferengi" {
		t.Logf("--> FAILED, expected 'Ferengi', got %s", r["name"].(string))
		t.Fail()
	}
	if ts, ok := r["ts"]; ok {
		if ts, ok := ts.(time.Time); ok {
			// time should not be zero because it defaults to current utc time
			if ts.IsZero() {
				t.Logf("--> FAILED: time is zero")
				t.Fail()
			} else if ts.Before(started) {
				t.Logf("--> FAILED: time %q is before start %q", ts, started)
				t.Fail()
			}
		} else {
			t.Logf("--> FAILED: ts is a real %T", ts)
			t.Fail()
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
		t.Logf("--> FAILED")
		t.Fail()
	}
	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if name != "Ferengi" {
		t.Logf("--> FAILED, name should be 'Ferengi' but it's '%s'", name)
		t.Fail()
	}
	qr.Next()
	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if name != "Cardassian" {
		t.Logf("--> FAILED, name should be 'Cardassian' but it's '%s'", name)
		t.Fail()
	}
	if ts != meeting {
		t.Logf("--> FAILED, time should be %q but it's %q", meeting, ts)
		t.Fail()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying Close")
	conn.Close()

	t.Logf("trying WriteOne after Close")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = wr

	t.Logf("trying Write after Close")
	t1 := make([]string, 0)
	t1 = append(t1, "DROP TABLE IF EXISTS "+testTableName()+"")
	t1 = append(t1, "DROP TABLE IF EXISTS "+testTableName()+"")
	wResults, err = conn.Write(t1)
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = wResults

	t.Logf("trying QueryOneParameterized after Close")
	qr, err = conn.QueryOneParameterized(
		ParameterizedStatement{
			Query: fmt.Sprintf("SELECT id FROM %s", testTableName()),
		},
	)
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = qr

	t.Logf("trying QueryParameterized after Close")
	_, err = conn.QueryParameterized(
		[]ParameterizedStatement{
			{
				Query: fmt.Sprintf("SELECT id FROM %s", testTableName()),
			},
			{
				Query: fmt.Sprintf("SELECT name FROM %s", testTableName()),
			},
			{
				Query: fmt.Sprintf("SELECT id, name FROM %s", testTableName()),
			},
		},
	)
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = qResults
}

func TestScanNullableTypes(t *testing.T) {
	var qr QueryResult
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying WriteOne DROP")
	_, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying WriteOne CREATE")
	_, err = conn.WriteOne("CREATE TABLE " + testTableName() + " (id integer, nullstring text, nullint64 integer, nullint32 integer, nullint16 integer, nullfloat64 real, nullbool integer, nulltime integer) strict")
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	t.Logf("trying Write INSERT")
	s := make([]string, 0)
	s = append(s, "INSERT INTO "+testTableName()+" (id) VALUES (1)") // other values are gonna be null
	s = append(s, "INSERT INTO "+testTableName()+" (id, nullstring, nullint64, nullint32, nullint16, nullfloat64, nullbool, nulltime) VALUES (2, 'Romulan', 1, 2, 3, 4.5, 1, "+met+")")
	_, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying QueryOne")
	qr, err = conn.QueryOne("SELECT id, nullstring, nullint64, nullint32, nullint16, nullfloat64, nullbool, nulltime FROM " + testTableName() + " WHERE id IN (1, 2)")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying Next()")
	na := qr.Next()
	if na != true {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Scan()")
	var id int64
	var nullString NullString
	var nullInt64 NullInt64
	var nullInt32 NullInt32
	var nullInt16 NullInt16
	var nullFloat64 NullFloat64
	var nullBool NullBool
	var nullTime NullTime
	err = qr.Scan(&id, &nullString, &nullInt64, &nullInt32, &nullInt16, &nullFloat64, &nullBool, &nullTime)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if id != 1 {
		t.Logf("--> FAILED, id should be 1 but it's %v", id)
		t.Fail()
	}
	if nullString.Valid || nullString.String != "" {
		t.Logf("--> FAILED, nullString should be invalid and unset but it's '%v' and '%v'", nullString.Valid, nullString.String)
		t.Fail()
	}
	if nullInt64.Valid || nullInt64.Int64 != 0 {
		t.Logf("--> FAILED, nullInt64 should be invalid and unset but it's '%v' and '%v'", nullInt64.Valid, nullInt64.Int64)
		t.Fail()
	}
	if nullInt32.Valid || nullInt32.Int32 != 0 {
		t.Logf("--> FAILED, nullInt32 should be invalid and unset but it's '%v' and '%v'", nullInt32.Valid, nullInt32.Int32)
		t.Fail()
	}
	if nullInt16.Valid || nullInt16.Int16 != 0 {
		t.Logf("--> FAILED, nullInt16 should be invalid and unset but it's '%v' and '%v'", nullInt16.Valid, nullInt16.Int16)
		t.Fail()
	}
	if nullFloat64.Valid || nullFloat64.Float64 != 0 {
		t.Logf("--> FAILED, nullFloat64 should be invalid and unset but it's '%v' and '%v'", nullFloat64.Valid, nullFloat64.Float64)
		t.Fail()
	}
	if nullBool.Valid || nullBool.Bool != false {
		t.Logf("--> FAILED, nullBool should be invalid and unset but it's '%v' and '%v'", nullBool.Valid, nullBool.Bool)
		t.Fail()
	}
	if nullTime.Valid || !nullTime.Time.IsZero() {
		t.Logf("--> FAILED, nullTime should be invalid and unset but it's '%v' and '%v'", nullTime.Valid, nullTime.Time)
		t.Fail()
	}

	t.Logf("trying Next()")
	qr.Next()
	if na != true {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Scan()")
	err = qr.Scan(&id, &nullString, &nullInt64, &nullInt32, &nullInt16, &nullFloat64, &nullBool, &nullTime)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if id != 2 {
		t.Logf("--> FAILED, id should be 2 but it's %v", id)
		t.Fail()
	}
	if !nullString.Valid || nullString.String != "Romulan" {
		t.Logf("--> FAILED, nullString should be valid and set to 'Romulan' but it's '%v'", nullString.String)
		t.Fail()
	}
	if !nullInt64.Valid || nullInt64.Int64 != 1 {
		t.Logf("--> FAILED, nullInt64 should be valid and set to 1 but it's '%v'", nullInt64.Int64)
		t.Fail()
	}
	if !nullInt32.Valid || nullInt32.Int32 != 2 {
		t.Logf("--> FAILED, nullInt32 should be valid and set to 2 but it's '%v'", nullInt32.Int32)
		t.Fail()
	}
	if !nullInt16.Valid || nullInt16.Int16 != 3 {
		t.Logf("--> FAILED, nullInt16 should be valid and set to 3 but it's '%v'", nullInt16.Int16)
		t.Fail()
	}
	if !nullFloat64.Valid || nullFloat64.Float64 != 4.5 {
		t.Logf("--> FAILED, nullFloat64 should be valid and set to 4.5 but it's '%v'", nullFloat64.Float64)
		t.Fail()
	}
	if !nullBool.Valid || nullBool.Bool != true {
		t.Logf("--> FAILED, nullBool should be valid and set to true but it's '%v'", nullBool.Bool)
		t.Fail()
	}
	if !nullTime.Valid || !nullTime.Time.Equal(meeting) {
		t.Logf("--> FAILED, nullTime should be valid and set to '%v' but it's '%v'", meeting, nullTime.Time)
		t.Fail()
	}

	t.Logf("trying WriteOne DROP")
	_, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying Close")
	conn.Close()
}
