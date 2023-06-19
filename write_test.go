package gorqlite_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/rqlite/gorqlite"
)

func TestWriteOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	wr, err := globalConnection.WriteOneContext(ctx, "CREATE TABLE "+testTableName()+" (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("creating table: %s - %s", err.Error(), wr.Err.Error())
	}

	t.Cleanup(func() {
		wr, err := globalConnection.WriteOne("DROP TABLE " + testTableName())
		if err != nil {
			t.Errorf("dropping table: %s - %s", err.Error(), wr.Err.Error())
		}
	})

	t.Run("WriteOne CREATE", func(t *testing.T) {
		tableName := RandStringBytes(8)

		wr, err := globalConnection.WriteOne("CREATE TABLE " + tableName + " (id integer, name text)")
		if err != nil {
			t.Errorf("failed during table creation: %s - %s", err.Error(), wr.Err.Error())
		}

		if err == nil && wr.Err != nil {
			t.Errorf("wr.Err must be nil if err is nil: %v", wr.Err.Error())
		}

		_, err = globalConnection.WriteOne("DROP TABLE IF EXISTS " + tableName)
		if err != nil {
			t.Errorf("failed during table deletion for: %s - %s", tableName, err.Error())
		}
	})

	t.Run("WriteOne DROP", func(t *testing.T) {
		// This is safe as the table was not found
		// (the keyword we're executing contains IF EXISTS clause)
		tableName := RandStringBytes(8)

		wr, err := globalConnection.WriteOne("DROP TABLE IF EXISTS " + tableName)
		if err != nil {
			t.Errorf("failed during table deletion: %v - %v", err.Error(), wr.Err)
		}

		if err == nil && wr.Err != nil {
			t.Errorf("wr.Err must be nil if err is nil: %v", wr.Err.Error())
		}
	})

	t.Run("WriteOne CTHULHU (should fail, bad SQL)", func(t *testing.T) {
		_, err := globalConnection.WriteOne("CTHULHU")
		if err == nil {
			t.Errorf("err must not be nil")
		}
	})

	t.Run("WriteOne INSERT", func(t *testing.T) {
		wr, err := globalConnection.WriteOne("INSERT INTO " + testTableName() + " (id, name) VALUES ( 1, 'aaa bbb ccc' )")
		if err != nil {
			t.Errorf("failed during insert: %v - %v", err.Error(), wr.Err.Error())
		}

		if err == nil && wr.Err != nil {
			t.Errorf("wr.Err must be nil if err is nil: %v", wr.Err.Error())
		}

		if wr.RowsAffected != 1 {
			t.Errorf("wr.RowsAffected must be 1")
		}
	})
}

func TestQueueOne(t *testing.T) {
	t.Cleanup(func() {
		r, err := globalConnection.WriteOne("DROP TABLE IF EXISTS " + testTableName())
		if err != nil {
			t.Errorf("dropping table: %s - %s", err.Error(), r.Err.Error())
		}
	})

	t.Run("QueueOne DROP", func(t *testing.T) {
		tableName := RandStringBytes(8)
		seq, err := globalConnection.QueueOne("DROP TABLE IF EXISTS " + tableName)
		if err != nil {
			t.Errorf("failed during table deletion: %v - %v", err.Error(), seq)
		}
	})

	t.Run("QueueOne CREATE", func(t *testing.T) {
		tableName := RandStringBytes(8)
		seq, err := globalConnection.QueueOne("CREATE TABLE " + tableName + " (id integer, name text)")
		if err != nil {
			t.Errorf("failed during table creation: %v - %v", err.Error(), seq)
		}

		seq, err = globalConnection.QueueOne("DROP TABLE IF EXISTS " + tableName)
		if err != nil {
			t.Errorf("failed during table deletion: %s - %d", err.Error(), seq)
		}
	})

	t.Run("QueueOne INSERT", func(t *testing.T) {
		seq, err := globalConnection.QueueOne("INSERT INTO " + testTableName() + " (id, name) VALUES ( 10, 'aaa bbb ccc' )")
		if err != nil {
			t.Errorf("failed during insert: %v - %v", err.Error(), seq)
		}

		if seq == 0 {
			t.Errorf("seq must not be 0")
		}
	})
}

func TestQueueOneContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	wr, err := globalConnection.WriteOneContext(ctx, "CREATE TABLE "+testTableName()+" (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("creating table: %s - %s", err.Error(), wr.Err.Error())
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		wr, err := globalConnection.WriteOneContext(ctx, "DROP TABLE "+testTableName())
		if err != nil {
			t.Errorf("dropping table: %s - %s", err.Error(), wr.Err.Error())
		}
	})

	seq, err := globalConnection.QueueOneContext(ctx, fmt.Sprintf("INSERT INTO "+testTableName()+" (id, name) VALUES ( %d, %s )", 120, "aaa bbb ccc"))
	if err != nil {
		t.Errorf("failed during insert: %v - %v", err.Error(), seq)
	}
}

func TestWriteOneParameterized(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	wr, err := globalConnection.WriteOneContext(ctx, "CREATE TABLE "+testTableName()+" (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("creating table: %s - %s", err.Error(), wr.Err.Error())
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		wr, err := globalConnection.WriteOneContext(ctx, "DROP TABLE "+testTableName())
		if err != nil {
			t.Errorf("dropping table: %s - %s", err.Error(), wr.Err.Error())
		}
	})

	t.Run("CTHULHU (should fail, bad SQL)", func(t *testing.T) {
		_, err := globalConnection.WriteOneParameterized(
			gorqlite.ParameterizedStatement{
				Query: "CTHULHU",
			},
		)
		if err == nil {
			t.Error("expected an error, got nil instead")
		}
	})

	t.Run("CREATE", func(t *testing.T) {
		_, err := globalConnection.WriteOneParameterized(
			gorqlite.ParameterizedStatement{
				Query: fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id integer, name text)", testTableName()),
			},
		)
		if err != nil {
			t.Errorf("creating table: %s", err.Error())
		}
	})

	t.Run("INSERT", func(t *testing.T) {
		wr, err := globalConnection.WriteOneParameterized(
			gorqlite.ParameterizedStatement{
				Query: fmt.Sprintf("INSERT INTO %s (id, name) VALUES ( 1, 'aaa bbb ccc' )", testTableName()),
			},
		)
		if err != nil {
			t.Errorf("insert query: %s", err.Error())
		}

		if wr.RowsAffected != 1 {
			t.Errorf("expected 1 row affected, got %d", wr.RowsAffected)
		}
	})
}

func TestQueueOneParameterized(t *testing.T) {
	_, err := globalConnection.QueueOneParameterized(
		gorqlite.ParameterizedStatement{
			Query: fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id integer, name text)", testTableName()),
		},
	)
	if err != nil {
		t.Errorf("failed during table creation: %s", err.Error())
	}

	t.Cleanup(func() {
		_, err := globalConnection.QueueOneParameterized(
			gorqlite.ParameterizedStatement{
				Query: fmt.Sprintf("DROP TABLE %s", testTableName()),
			},
		)
		if err != nil {
			t.Errorf("failed during dropping table: %s", err.Error())
		}
	})

	t.Run("QueryOneParameterized INSERT", func(t *testing.T) {
		seq, err := globalConnection.QueueOneParameterized(gorqlite.ParameterizedStatement{
			Query: fmt.Sprintf("INSERT INTO %s (id, name) VALUES ( 1, 'aaa bbb ccc' )", testTableName()),
		})
		if err != nil {
			t.Errorf("failed during insert: %s", err.Error())
		}
		if seq == 0 {
			t.Error("expected a sequence ID, got 0")
		}
	})
}

func TestWrite(t *testing.T) {

	t.Cleanup(func() {
		wr, err := globalConnection.WriteOne("DROP TABLE IF EXISTS " + testTableName())
		if err != nil {
			t.Errorf("dropping table: %s - %s", err.Error(), wr.Err.Error())
		}
	})

	t.Run("Write DROP & CREATE", func(t *testing.T) {
		tableName := RandStringBytes(8)
		s := make([]string, 0)
		s = append(s, "CREATE TABLE "+tableName+" (id integer, name text)")
		s = append(s, "DROP TABLE IF EXISTS "+tableName)
		results, err := globalConnection.Write(s)
		if err != nil {
			t.Errorf("failed during table creation: %v - %v", err.Error(), results[0].Err.Error())
		}
	})

	t.Run("Write INSERT", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		wr, err := globalConnection.WriteOneContext(ctx, "CREATE TABLE "+testTableName()+" (id INTEGER, name TEXT)")
		if err != nil {
			t.Fatalf("creating table: %s - %s", err.Error(), wr.Err.Error())
		}

		s := make([]string, 0)
		s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 21, 'aaa bbb ccc' )")
		s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 22, 'ddd eee fff' )")
		s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 23, 'ggg hhh iii' )")
		s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 24, 'jjj kkk lll' )")
		results, err := globalConnection.Write(s)
		if err != nil {
			for _, result := range results {
				if result.Err != nil {
					t.Errorf("result error: %v", result.Err)
				}
			}
		}

		if len(results) != 4 {
			t.Errorf("expected results to be length of 4, got: %d", len(results))
		}
	})
}

func TestWriteParameterized(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	wr, err := globalConnection.WriteOneContext(ctx, "CREATE TABLE "+testTableName()+" (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("creating table: %s - %s", err.Error(), wr.Err.Error())
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		wr, err := globalConnection.WriteOneContext(ctx, "DROP TABLE IF EXISTS "+testTableName())
		if err != nil {
			t.Errorf("dropping table: %s - %s", err.Error(), wr.Err.Error())
		}
	})

	t.Run("DROP", func(t *testing.T) {
		tableName := RandStringBytes(8)
		_, err := globalConnection.WriteParameterized(
			[]gorqlite.ParameterizedStatement{
				{
					Query: fmt.Sprintf("CREATE TABLE %s (id integer, name text)", tableName),
				},
				{
					Query: fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName),
				},
			},
		)
		if err != nil {
			t.Errorf("failed during dropping & creating table: %s", err.Error())
		}
	})

	t.Run("INSERT", func(t *testing.T) {
		results, err := globalConnection.WriteParameterized(
			[]gorqlite.ParameterizedStatement{
				{
					Query: "CREATE TABLE IF NOT EXISTS " + testTableName() + " (id INTEGER, name TEXT)",
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
				{
					Query:     fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
					Arguments: []interface{}{1, "aaa bbb ccc"},
				},
			},
		)
		if err != nil {
			t.Errorf("writing multiple parameterized queries: %s", err.Error())
		}

		if len(results) != 5 {
			t.Errorf("expected 4 results, got %d", len(results))
		}
	})
}

func TestWrites(t *testing.T) {
	t.Logf("trying Open")
	conn, err := gorqlite.Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying Write DROP & CREATE")
	results, err := conn.Write([]string{
		"DROP TABLE IF EXISTS " + testTableName() + "",
		"CREATE TABLE " + testTableName() + " (id integer, name text)",
	})
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	t.Cleanup(func() {
		_, err = conn.Write([]string{
			"DROP TABLE IF EXISTS " + testTableName() + "",
		})
	})

	t.Logf("trying Write INSERT")
	insert := "INSERT INTO " + testTableName() + " (id, name) VALUES ( ?, ? )"
	s := make([]*gorqlite.Statement, 0)
	s = append(s, gorqlite.NewStatement(insert, 1, "aaa bbb ccc"))
	s = append(s, gorqlite.NewStatement(insert, 2, "ddd eee fff"))
	s = append(s, gorqlite.NewStatement(insert, 3, "ggg hhh iii"))
	s = append(s, gorqlite.NewStatement(insert, 4, "jjj kkk lll"))
	results, err = conn.WriteStmt(context.Background(), s...)
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if len(results) != 4 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Write DROP")
	results, err = conn.Write([]string{"DROP TABLE IF EXISTS " + testTableName()})
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

}

func TestRequests(t *testing.T) {
	t.Logf("trying Open")
	conn, err := gorqlite.Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying Write DROP & CREATE")
	results, err := conn.Write([]string{
		"DROP TABLE IF EXISTS " + testTableName() + "",
		"CREATE TABLE " + testTableName() + " (id integer, name text)",
	})
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	t.Cleanup(func() {
		_, err = conn.Write([]string{
			"DROP TABLE IF EXISTS " + testTableName() + "",
		})
	})

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
	if results[1].RowsAffected != 1 {
		t.Errorf("expected 1 row changed, got %d", results[1].RowsAffected)
	}

	t.Logf("trying Write INSERT & REQUEST")
	insert := "INSERT INTO " + testTableName() + " (id, name) VALUES ( ?, ? )"
	s := make([]*gorqlite.Statement, 0)
	s = append(s, gorqlite.NewStatement(insert, 1, "aaa bbb ccc"))
	s = append(s, gorqlite.NewStatement(insert, 2, "ddd eee fff"))
	s = append(s, gorqlite.NewStatement(insert, 3, "ggg hhh iii"))
	s = append(s, gorqlite.NewStatement(insert, 4, "jjj kkk lll"))
	update := "UPDATE " + testTableName() + " SET name=? WHERE id=? RETURNING id"
	s = append(s, gorqlite.NewStatement(update, "tony", 1).WithReturning(true))
	get := "SELECT id FROM " + testTableName() + " WHERE id=? "
	s = append(s, gorqlite.NewStatement(get, 1))

	reqResults, err := conn.RequestStmt(context.Background(), s...)
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if len(reqResults) != 6 {
		t.Logf("--> FAILED - expected 6 results, got %v", len(reqResults))
		t.Fail()
	}
	for i := 0; i < 4; i++ {
		if !reqResults[i].Query.IsZero() {
			t.Errorf("expected NO query result for request %d", i)
		}
		if reqResults[i].Write.IsZero() {
			t.Errorf("expected write result for request %d", i)
		}
	}
	for i := 4; i < 6; i++ {
		if !reqResults[i].Write.IsZero() {
			t.Errorf("expected NO write result for request %d", i)
		}
		if reqResults[i].Query.IsZero() {
			t.Errorf("expected query result for request %d", i)
		}
		if len(reqResults[i].Query.Columns()) != 1 {
			t.Errorf("expected query result of request %d with 1 column, got %d", i, len(reqResults[i].Query.Columns()))
		}
		if len(reqResults[i].Query.Types()) != 1 {
			t.Errorf("expected query result of request %d with 1 type, got %d", i, len(reqResults[i].Query.Types()))
		}
		if reqResults[i].Query.NumRows() != int64(1) {
			t.Errorf("expected query result of request %d with 1 value, got %d", i, reqResults[i].Query.NumRows())
		}
		val := 0
		if !reqResults[i].Query.Next() {
			t.Errorf("expected 1 query result of request %d", i)
		}
		err = reqResults[i].Query.Scan(&val)
		if err != nil {
			t.Errorf("expected int query result of request %d", i)
		}
		if val != 1 {
			t.Errorf("expected query result of request %d value 1, got %d", i, val)
		}
	}

	t.Logf("trying Write DROP")
	results, err = conn.Write([]string{"DROP TABLE IF EXISTS " + testTableName()})
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
}

func TestReadWriteLargeNumbers(t *testing.T) {
	conn, err := gorqlite.Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	_, err = conn.Write([]string{
		"DROP TABLE IF EXISTS " + testTableName() + "",
		"CREATE TABLE " + testTableName() + " (id integer, nanos integer, length double, name text)",
	})
	if err != nil {
		t.Logf("--> CREATE TABLE FAILED %v", err)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err = conn.Write([]string{
			"DROP TABLE IF EXISTS " + testTableName() + "",
		})
	})

	type thing struct {
		id     int64
		nanos  int64
		length float64
		name   string
	}

	now := time.Now().UnixNano()
	makeLen := func(n int64) float64 {
		return float64(n)/10000.0 + 0.45
	}
	toInsert := []*thing{
		{id: 1, nanos: now + 1, length: makeLen(now) + 0.1, name: "aaa"},
		{id: 2, nanos: now + 2, length: makeLen(now) + 0.2, name: "bbb"},
		{id: 3, nanos: now + 3, length: makeLen(now) + 0.3, name: "ccc"},
		{id: 4, nanos: now + 4, length: makeLen(now) + 0.4, name: "ddd"},
	}

	t.Logf("trying Write INSERT")
	insert := "INSERT INTO " + testTableName() + " (id, nanos, length, name) VALUES ( ?, ?, ?, ? )"
	s := make([]*gorqlite.Statement, 0)
	for _, ti := range toInsert {
		s = append(s, gorqlite.NewStatement(insert, ti.id, ti.nanos, ti.length, ti.name))
	}
	_, err = conn.WriteStmt(context.Background(), s...)
	if err != nil {
		t.Logf("--> INSERT FAILED %v", err)
		t.Fatal(err)
	}

	qrs, err := conn.QueryStmt(
		context.Background(),
		gorqlite.NewStatement("SELECT id, nanos, length, name FROM "+testTableName()))
	if err != nil {
		t.Logf("--> QUERY FAILED %v", err)
		t.Fatal(err)
	}
	if len(qrs) != 1 {
		t.Fatal("--> QUERY FAILED expected 1 result, got ", len(qrs))
	}
	qr := qrs[0]

	ret := make([]*thing, 0)
	for qr.Next() {
		s := &thing{}
		err = qr.Scan(&s.id, &s.nanos, &s.length, &s.name)
		if err != nil {
			t.Logf("--> SCAN FAILED %v", err)
			t.Fatal(err)
		}
		ret = append(ret, s)
	}
	if len(ret) != len(toInsert) {
		t.Fatal(fmt.Sprintf("--> QUERY FAILED expected %d things, got %d", len(toInsert), len(ret)))
	}
	//for _, r := range ret {
	//	fmt.Println(r.id, r.nanos, fmt.Sprintf("%.3f", r.length), r.name)
	//}
	for i, ti := range toInsert {
		if !reflect.DeepEqual(ti, ret[i]) {
			t.Fatal(fmt.Sprintf("--> expected equal %#v and %#v", ti, ret[i]))
		}
	}
}
