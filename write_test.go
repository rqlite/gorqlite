package gorqlite_test

import (
	"testing"

	"github.com/rqlite/gorqlite"
)

// import "os"

func TestWriteOne(t *testing.T) {
	var wr gorqlite.WriteResult
	var err error

	t.Logf("trying Open")
	conn, err := gorqlite.Open(testUrl())
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

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
}

func TestWriteOneQueued(t *testing.T) {
	var seq int64
	var err error

	t.Logf("trying Open")
	conn, err := gorqlite.Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying QueueOne DROP")
	seq, err = conn.QueueOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying QueueOne CREATE")
	seq, err = conn.QueueOne("CREATE TABLE " + testTableName() + " (id integer, name text)")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying QueueOne INSERT")
	seq, err = conn.QueueOne("INSERT INTO " + testTableName() + " (id, name) VALUES ( 1, 'aaa bbb ccc' )")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("checking QueueOne sequence ID")
	if seq == 0 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying QueueOne DROP")
	seq, err = conn.QueueOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
}

func TestWrite(t *testing.T) {
	var results []gorqlite.WriteResult
	var err error
	var s []string

	t.Logf("trying Open")
	conn, err := gorqlite.Open(testUrl())
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
		t.Fatal(err)
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
		for _, result := range results {
			if result.Err != nil {
				t.Error(result.Err)
			}
		}

		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Logf("--> FAILED")
		t.Fatal("result does not equal to 4")
	}

	t.Logf("trying Write DROP")
	s = make([]string, 0)
	s = append(s, "DROP TABLE IF EXISTS "+testTableName()+"")
	results, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FAILED")
		t.Fatal(err)
	}
}
