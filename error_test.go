package gorqlite

import (
	"errors"
	"testing"
)

func TestStatementErrors(t *testing.T) {
	t.Run("returns an empty string if there are no errors", func(t *testing.T) {
		var err StatementErrors
		expected := ""
		actual := err.Error()
		if actual != expected {
			t.Errorf("expected %q, got %q", expected, actual)
		}
	})
	t.Run("returns a string representation of the statement errors", func(t *testing.T) {
		err := StatementErrors{
			errors.New("error 1"),
			errors.New("error 2"),
		}
		expected := "there were 2 statement errors\nerror 1\nerror 2"
		actual := err.Error()
		if actual != expected {
			t.Errorf("expected %q, got %q", expected, actual)
		}
	})
	t.Run("can unwrap the slice of statement errors", func(t *testing.T) {
		var errs StatementErrors
		var err1 = errors.New("error 1")
		var err2 = errors.New("error 2")
		errs = append(errs, err1, err2)
		expected := []error{err1, err2}
		actual := errs.Unwrap()
		if len(actual) != len(expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		for i, err := range actual {
			if err != expected[i] {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	})
}
