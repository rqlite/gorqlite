package gorqlite

import (
	"fmt"
	"testing"
)

func TestWriteOne(t *testing.T) {
	var wr WriteResult
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying WriteOne CTHULHU (should fail, bad SQL)")
	wr, err = conn.WriteOne("CTHULHU")
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne CREATE")
	wr, err = conn.WriteOne("CREATE TABLE " + testTableName() + " (id integer, name text)")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying WriteOne INSERT")
	wr, err = conn.WriteOne("INSERT INTO " + testTableName() + " (id, name) VALUES ( 1, 'aaa bbb ccc' )")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("checking WriteOne RowsAffected")
	if wr.RowsAffected != 1 {
		t.Logf("--> FAILED, expected 1, got %d", wr.RowsAffected)
		t.Fail()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
}

func TestQueueOne(t *testing.T) {
	var seq int64
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying QueueOne DROP")
	seq, err = conn.QueueOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying QueueOne CREATE")
	seq, err = conn.QueueOne("CREATE TABLE " + testTableName() + " (id integer, name text)")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying QueueOne INSERT")
	seq, err = conn.QueueOne("INSERT INTO " + testTableName() + " (id, name) VALUES ( 1, 'aaa bbb ccc' )")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("checking QueueOne sequence ID")
	if seq == 0 {
		t.Logf("--> FAILED, expected non-zero, got %d", seq)
		t.Fail()
	}

	t.Logf("trying QueueOne DROP")
	seq, err = conn.QueueOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
}

func TestWriteOneParameterized(t *testing.T) {
	var wr WriteResult
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying WriteOneParameterized DROP")
	wr, err = conn.WriteOneParameterized(
		ParameterizedStatement{
			Query: fmt.Sprintf("DROP TABLE IF EXISTS %s", testTableName()),
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying WriteOneParameterized CTHULHU (should fail, bad SQL)")
	wr, err = conn.WriteOneParameterized(
		ParameterizedStatement{
			Query: "CTHULHU",
		},
	)
	if err == nil {
		t.Logf("--> FAILED, expected an error but got none")
		t.Fail()
	}

	t.Logf("trying WriteOneParameterized CREATE")
	wr, err = conn.WriteOneParameterized(
		ParameterizedStatement{
			Query: fmt.Sprintf("CREATE TABLE %s (id integer, name text)", testTableName()),
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying WriteOneParameterized INSERT")
	wr, err = conn.WriteOneParameterized(
		ParameterizedStatement{
			Query: fmt.Sprintf("INSERT INTO %s (id, name) VALUES ( 1, 'aaa bbb ccc' )", testTableName()),
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("checking WriteOneParameterized RowsAffected")
	if wr.RowsAffected != 1 {
		t.Logf("--> FAILED, expected 1 row affected, got %d", wr.RowsAffected)
		t.Fail()
	}

	t.Logf("trying WriteOneParameterized DROP")
	wr, err = conn.WriteOneParameterized(
		ParameterizedStatement{
			Query: fmt.Sprintf("DROP TABLE IF EXISTS %s", testTableName()),
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
}

func TestQueueOneParameterized(t *testing.T) {
	var seq int64
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying QueueOneParameterized DROP")
	seq, err = conn.QueueOneParameterized(
		ParameterizedStatement{
			Query: fmt.Sprintf("DROP TABLE IF EXISTS %s", testTableName()),
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying QueueOneParameterized CREATE")
	seq, err = conn.QueueOneParameterized(
		ParameterizedStatement{
			Query: fmt.Sprintf("CREATE TABLE %s (id integer, name text)", testTableName()),
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying QueueOneParameterized INSERT")
	seq, err = conn.QueueOneParameterized(ParameterizedStatement{
		Query: fmt.Sprintf("INSERT INTO %s (id, name) VALUES ( 1, 'aaa bbb ccc' )", testTableName()),
	})
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("checking QueueOneParameterized sequence ID")
	if seq == 0 {
		t.Logf("--> FAILED, expected a sequence ID, got 0")
		t.Fail()
	}

	t.Logf("trying QueueOneParameterized DROP")
	seq, err = conn.QueueOneParameterized(
		ParameterizedStatement{
			Query: fmt.Sprintf("DROP TABLE IF EXISTS %s", testTableName()),
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
}

func TestWrite(t *testing.T) {
	var results []WriteResult
	var err error
	var s []string

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying Write DROP & CREATE")
	s = make([]string, 0)
	s = append(s, "DROP TABLE IF EXISTS "+testTableName()+"")
	s = append(s, "CREATE TABLE "+testTableName()+" (id integer, name text)")
	results, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying Write INSERT")
	s = make([]string, 0)
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 1, 'aaa bbb ccc' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 2, 'ddd eee fff' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 3, 'ggg hhh iii' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 4, 'jjj kkk lll' )")
	results, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if len(results) != 4 {
		t.Logf("--> FAILED, expected 4 results, got %d", len(results))
		t.Fail()
	}

	t.Logf("trying Write DROP")
	s = make([]string, 0)
	s = append(s, "DROP TABLE IF EXISTS "+testTableName()+"")
	results, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
}

func TestWriteParameterized(t *testing.T) {
	var results []WriteResult
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying WriteParameterized DROP & CREATE")
	results, err = conn.WriteParameterized(
		[]ParameterizedStatement{
			{
				Query: fmt.Sprintf("DROP TABLE IF EXISTS %s", testTableName()),
			},
			{
				Query: fmt.Sprintf("CREATE TABLE %s (id integer, name text)", testTableName()),
			},
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}

	t.Logf("trying WriteParameterized INSERT")
	results, err = conn.WriteParameterized(
		[]ParameterizedStatement{
			{
				Query:     fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
				Arguments: []interface{}{1, "aaa bbb ccc"},
			},
			{
				Query:     fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
				Arguments: []interface{}{1, "aaa bbb ccc"},
			},
			{
				Query:     fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
				Arguments: []interface{}{1, "aaa bbb ccc"},
			},
			{
				Query:     fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
				Arguments: []interface{}{1, "aaa bbb ccc"},
			},
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if len(results) != 4 {
		t.Logf("--> FAILED, expected 4 results, got %d", len(results))
		t.Fail()
	}

	t.Logf("trying WriteParameterized DROP")
	results, err = conn.WriteParameterized(
		[]ParameterizedStatement{
			{
				Query: fmt.Sprintf("DROP TABLE IF EXISTS %s", testTableName()),
			},
		},
	)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
}
