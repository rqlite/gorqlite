package gorqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/rqlite/gorqlite"
)

func TestClosedConnection(t *testing.T) {
	conn, err := gorqlite.Open(testUrl())
	if err != nil {
		t.Errorf("failed to open connection: %v", err.Error())
	}

	// Now we close it
	conn.Close()

	t.Run("ConsistencyLevel", func(t *testing.T) {
		_, err := conn.ConsistencyLevel()
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("Leader", func(t *testing.T) {
		_, err := conn.Leader()
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("Peers", func(t *testing.T) {
		_, err := conn.Peers()
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("SetConsistencyLevel", func(t *testing.T) {
		err := conn.SetConsistencyLevel(gorqlite.ConsistencyLevelNone)
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("SetExecutionWithTransaction", func(t *testing.T) {
		err := conn.SetExecutionWithTransaction(true)
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("WriteOne", func(t *testing.T) {
		_, err := conn.WriteOne("")
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("WriteOneContext", func(t *testing.T) {
		_, err := conn.WriteOneContext(context.Background(), "")
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("WriteOneParameterized", func(t *testing.T) {
		_, err := conn.WriteOneParameterized(gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("WriteOneParameterizedContext", func(t *testing.T) {
		_, err := conn.WriteOneParameterizedContext(context.Background(), gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueueOne", func(t *testing.T) {
		_, err := conn.QueueOne("")
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueueOneContext", func(t *testing.T) {
		_, err := conn.QueueOneContext(context.Background(), "")
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueueOneParameterized", func(t *testing.T) {
		_, err := conn.QueueOneParameterized(gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueueOneParameterizedContext", func(t *testing.T) {
		_, err := conn.QueueOneParameterizedContext(context.Background(), gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("Write", func(t *testing.T) {
		_, err := conn.Write([]string{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("WriteContext", func(t *testing.T) {
		_, err := conn.WriteContext(context.Background(), []string{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("WriteParameterized", func(t *testing.T) {
		_, err := conn.WriteParameterized([]gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("WriteParameterizedContext", func(t *testing.T) {
		_, err := conn.WriteParameterizedContext(context.Background(), []gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("Queue", func(t *testing.T) {
		_, err := conn.Queue([]string{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueueContext", func(t *testing.T) {
		_, err := conn.QueueContext(context.Background(), []string{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueueParameterized", func(t *testing.T) {
		_, err := conn.QueueParameterized([]gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueueParameterizedContext", func(t *testing.T) {
		_, err := conn.QueueParameterizedContext(context.Background(), []gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueryOne", func(t *testing.T) {
		_, err := conn.QueryOne("")
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueryOneContext", func(t *testing.T) {
		_, err := conn.QueryOneContext(context.Background(), "")
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueryOneParameterized", func(t *testing.T) {
		_, err := conn.QueryOneParameterized(gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueryOneParameterizedContext", func(t *testing.T) {
		_, err := conn.QueryOneParameterizedContext(context.Background(), gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("Query", func(t *testing.T) {
		_, err := conn.Query([]string{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueryContext", func(t *testing.T) {
		_, err := conn.QueryContext(context.Background(), []string{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueryParameterized", func(t *testing.T) {
		_, err := conn.QueryParameterized([]gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})

	t.Run("QueryParameterizedContext", func(t *testing.T) {
		_, err := conn.QueryParameterizedContext(context.Background(), []gorqlite.ParameterizedStatement{})
		if err == nil {
			t.Errorf("expected error, got nil")
		}

		if !errors.Is(err, gorqlite.ErrClosed) {
			t.Errorf("expected error to be ErrClosed, got %v", err)
		}
	})
}
