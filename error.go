package gorqlite

import (
	"errors"
	"fmt"
	"strings"
)

var _ error = StatementErrors{}

type StatementErrors []error

func joinErrors(errs ...error) error {
	if len(errs) == 0 {
		return nil
	}
	var se StatementErrors
	for _, err := range errs {
		if err == nil {
			continue
		}
		se = append(se, err)
	}
	if len(se) == 0 {
		return nil
	}
	return se
}

// Error returns a string representation of the statement errors.
func (errs StatementErrors) Error() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("there were %d statement errors", len(errs)))
	for _, err := range errs {
		sb.WriteString("\n")
		sb.WriteString(err.Error())
	}
	return sb.String()
}

// Unwrap returns the slice of statement errors.
func (errs StatementErrors) Unwrap() []error {
	return errs
}

// Is returns true if the current error, or one of the statement errors is equal to the target error.
func (errs StatementErrors) Is(target error) bool {
	for _, err := range errs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// As returns true if the current error, or one of the statement errors can be assigned to the target error.
func (errs StatementErrors) As(target interface{}) bool {
	for _, err := range errs {
		if errors.As(err, target) {
			return true
		}
	}
	return false
}
