package gorqlite_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rqlite/gorqlite/v2"
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

		wr, err := globalConnection.WriteOneContext(ctx, "DROP TABLE "+testTableName())
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

		if len(results) != 4 {
			t.Errorf("expected 4 results, got %d", len(results))
		}
	})
}
