package gorqlite

import (
	"errors"
	"testing"
)

func TestStatementErrors(t *testing.T) {
	t.Run("returns nil when there are no errors", func(t *testing.T) {
		err := joinErrors()
		if err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	})
	t.Run("returns nil when there are only nil errors", func(t *testing.T) {
		err := joinErrors(nil, nil)
		if err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	})
	t.Run("ignores nil errors", func(t *testing.T) {
		err := joinErrors(errors.New("error 1"), nil, errors.New("error 2"))
		expected := "there were 2 statement errors\nerror 1\nerror 2"
		actual := err.Error()
		if actual != expected {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})
	t.Run("returns a string representation of the statement errors", func(t *testing.T) {
		err := joinErrors(errors.New("error 1"), errors.New("error 2"))
		expected := "there were 2 statement errors\nerror 1\nerror 2"
		actual := err.Error()
		if actual != expected {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})
	t.Run("can unwrap the slice of statement errors", func(t *testing.T) {
		var err1 = errors.New("error 1")
		var err2 = errors.New("error 2")
		err := joinErrors(err1, err2)
		expected := []error{err1, err2}
		unwrap, canUnwrap := err.(interface{ Unwrap() []error })
		if !canUnwrap {
			t.Fatal("cannot unwrap the statement errors")
		}
		actual := unwrap.Unwrap()
		if len(actual) != len(expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
		for i, err := range actual {
			if err != expected[i] {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	})
	t.Run("can check if an error is wrapped, but present", func(t *testing.T) {
		var err1 = errors.New("error 1")
		var err2 = errors.New("error 2")
		err := joinErrors(err1, err2)
		if !errors.Is(err, err1) {
			t.Errorf("expected true, got false")
		}
	})
	t.Run("can check if an error is wrapped, but not present", func(t *testing.T) {
		var err1 = errors.New("error 1")
		var err2 = errors.New("error 2")
		err := joinErrors(err1)
		if errors.Is(err, err2) {
			t.Errorf("expected false, got true")
		}
	})
	t.Run("can assign the error to a target", func(t *testing.T) {
		var err1 = errors.New("error 1")
		var err2 = testError{msg: "error 2"}
		err := joinErrors(err1, err2)

		var target testError
		if !errors.As(err, &target) {
			t.Errorf("expected true, got false")
		}
		if target != err2 {
			t.Errorf("expected %v, got %v", err2, target)
		}
	})
	t.Run("does not assign if the target is not the same type", func(t *testing.T) {
		var err1 = errors.New("error 1")
		var err2 = errors.New("error 2")
		err := joinErrors(err1, err2)

		var target testError
		if errors.As(err, &target) {
			t.Errorf("expected false, got true")
		}
	})
}

type testError struct {
	msg string
}

func (e testError) Error() string {
	return e.msg
}
