package gorqlite_test

import (
	"context"
	"testing"
	"time"
)

// import "os"

func TestWriteOne(t *testing.T) {
	t.Run("WriteOne CREATE", func(t *testing.T) {
		tableName := RandStringBytes(8)

		wr, err := globalConnection.WriteOne("CREATE TABLE " + tableName + " (id integer, name text)")
		if err != nil {
			t.Errorf("failed during table creation: %v - %v", err.Error(), wr.Err.Error())
		}

		if err == nil && wr.Err != nil {
			t.Errorf("wr.Err must be nil if err is nil: %v", wr.Err.Error())
		}
	})

	t.Run("WriteOne DROP", func(t *testing.T) {
		tableName := RandStringBytes(8)

		wr, err := globalConnection.WriteOne("DROP TABLE IF EXISTS " + tableName + "")
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

	t.Run("WriteOne INSERT parameterized", func(t *testing.T) {
		wr, err := globalConnection.WriteOne("INSERT INTO "+testTableName()+" (id, name) VALUES ( ?, ? )", 2, "aaa bbb ccc")
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

func TestWriteOneQueued(t *testing.T) {
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

	t.Run("QueueOne INSERT parameterized", func(t *testing.T) {
		seq, err := globalConnection.QueueOne("INSERT INTO "+testTableName()+" (id, name) VALUES ( ?, ? )", 11, "aaa bbb ccc")
		if err != nil {
			t.Errorf("failed during insert: %v - %v", err.Error(), seq)
		}

		if seq == 0 {
			t.Errorf("seq must not be 0")
		}
	})
}

func TestQueueOneContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	seq, err := globalConnection.QueueOneContext(ctx, "INSERT INTO "+testTableName()+" (id, name) VALUES ( ?, ? )", 120, "aaa bbb ccc")
	if err != nil {
		t.Errorf("failed during insert: %v - %v", err.Error(), seq)
	}
}

func TestWrite(t *testing.T) {
	t.Run("Write DROP & CREATE", func(t *testing.T) {
		tableName := RandStringBytes(8)
		s := make([]string, 0)
		s = append(s, "DROP TABLE IF EXISTS "+tableName)
		s = append(s, "CREATE TABLE "+tableName+" (id integer, name text)")
		results, err := globalConnection.Write(s)
		if err != nil {
			t.Errorf("failed during table creation: %v - %v", err.Error(), results[0].Err.Error())
		}
	})

	t.Run("Write INSERT", func(t *testing.T) {
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
