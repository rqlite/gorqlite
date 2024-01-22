package gorqlite_test

import (
	"context"
	"fmt"
	"github.com/rqlite/gorqlite"
	"testing"
	"time"
)

func TestRequestParameterized(t *testing.T) {
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
		_, err := globalConnection.RequestParameterized(
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

	t.Run("INSERT and SELECT", func(t *testing.T) {
		results, err := globalConnection.RequestParameterized(
			[]gorqlite.ParameterizedStatement{
				{
					Query:     fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
					Arguments: []interface{}{1, "aaa"},
				},
				{
					Query:     fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName()),
					Arguments: []interface{}{2, "bbb"},
				},
				{
					Query:     fmt.Sprintf("SELECT id, name FROM %s WHERE name > ?", testTableName()),
					Arguments: []interface{}{"aaa"},
				},
			},
		)
		if err != nil {
			t.Errorf("making multiple parameterized requests: %s", err.Error())
		}

		if len(results) != 3 {
			t.Errorf("expected 3 results, got %d", len(results))
		}

		res := results[0]
		wr := res.Write
		if wr == nil {
			t.Error("unexpected results[0] type")
		}
		if wr.RowsAffected != 1 {
			t.Errorf("results[0].Write.RowsAffected must be 1")
		}

		res = results[1]
		wr = res.Write
		if wr == nil {
			t.Error("unexpected results[1] type")
		}
		if wr.RowsAffected != 1 {
			t.Errorf("results[1].Write.RowsAffected must be 1")
		}

		res = results[2]
		qr := res.Query
		if qr == nil {
			t.Error("unexpected results[2] type")
		}
		if qr.NumRows() != 1 {
			t.Errorf("failed to scan from results[2].Query.NumRows() must be 1, not %d", qr.NumRows())
		}
		qr.Next()
		var id int
		var name string
		if err := qr.Scan(&id, &name); err != nil {
			t.Errorf("failed to scan from results[2].Query: %v", err)
		}
		if id != 2 || name != "bbb" {
			t.Errorf("incorrect results[2].Query result: expected id == 2 && name == bbb, got id == %d && name == %s", id, name)
		}
	})
}
