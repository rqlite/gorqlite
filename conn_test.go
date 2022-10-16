package gorqlite_test

import (
	"testing"

	"github.com/rqlite/gorqlite"
)

func TestSetConsistencyLevel(t *testing.T) {
	conn, err := gorqlite.Open(testUrl())
	if err != nil {
		t.Fatalf("failed to open connection: %v", err.Error())
	}

	t.Run("Less than none", func(t *testing.T) {
		err := conn.SetConsistencyLevel(-1)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("Greater than strong", func(t *testing.T) {
		err := conn.SetConsistencyLevel(100)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("None", func(t *testing.T) {
		err := conn.SetConsistencyLevel(gorqlite.ConsistencyLevelNone)
		if err != nil {
			t.Errorf("expected nil, got %v", err)
		}

		currentLevel, err := conn.ConsistencyLevel()
		if err != nil {
			t.Errorf("unexpected error: %s", err.Error())
		}

		if currentLevel != "none" {
			t.Errorf("expected currentLevel to be 'none', instead got %s", currentLevel)
		}
	})

	conn.Close()
}

func TestSetExecutionWithTransaction(t *testing.T) {
	conn, err := gorqlite.Open(testUrl())
	if err != nil {
		t.Fatalf("failed to open connection: %v", err.Error())
	}

	err = conn.SetExecutionWithTransaction(true)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	conn.Close()
}
