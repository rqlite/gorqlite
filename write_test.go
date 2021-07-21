package gorqlite

import (
	"fmt"
	"testing"
)

// import "os"

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
		t.Logf("--> FAILED")
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
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne INSERT")
	wr, err = conn.WriteOne("INSERT INTO " + testTableName() + " (id, name) VALUES ( 1, 'aaa bbb ccc' )")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("checking WriteOne RowsAffected")
	if wr.RowsAffected != 1 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne INSERT")
	wr, err = conn.WriteOnePrepared(
		&PreparedStatement{
			Query:     fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
			Arguments: []interface{}{1, "aaa bbb ccc"},
		},
	)

	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("checking WriteOnePrepared RowsAffected")
	if wr.RowsAffected != 1 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED")
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
		t.Logf("--> FAILED")
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
		t.Logf("--> FAILED")
		t.Fail()
	}
	if len(results) != 4 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Write INSERT")
	results, err = conn.WritePrepared(
		[]*PreparedStatement{
			{
				Query: fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
				Arguments: []interface{}{1, "aaa bbb ccc"},
			},
			{
				Query: fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
				Arguments: []interface{}{1, "aaa bbb ccc"},
			},
			{
				Query: fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
				Arguments: []interface{}{1, "aaa bbb ccc"},
			},
			{
				Query: fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
				Arguments: []interface{}{1, "aaa bbb ccc"},
			},
		},
	)
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if len(results) != 4 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Write DROP")
	s = make([]string, 0)
	s = append(s, "DROP TABLE IF EXISTS "+testTableName()+"")
	results, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

}
