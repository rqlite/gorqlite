package gorqlite

import (
	"fmt"
	"strings"
)

var _ error = StatementErrors{}

type StatementErrors []error

// Error returns a string representation of the statement errors.
func (errs StatementErrors) Error() string {
	if len(errs) == 0 {
		return ""
	}

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
