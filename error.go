package gorqlite

import (
	"errors"
	"fmt"
	"strings"
)

var _ error = StatementErrors{}

// StatementError is the error rqlite reports for a single statement that
// failed to execute. It is the concrete type behind QueryResult.Err,
// WriteResult.Err and RequestResult.Err whenever the failure was reported
// by rqlite rather than by gorqlite itself, so callers can recover the
// SQLite result codes with errors.As:
//
//	var se *gorqlite.StatementError
//	if errors.As(err, &se) && se.SQLite != nil && se.SQLite.Code == 19 {
//		// constraint violation
//	}
//
// The error returned by Query, Write and Request for a batch is a
// StatementErrors, and errors.As looks through it to the first matching
// StatementError.
type StatementError struct {
	// Message is the error text reported by rqlite.
	Message string `json:"message"`

	// SQLite holds the result codes SQLite reported, when the failure
	// came from SQLite itself. It is nil for other failures, such as a
	// timeout or a parameter conversion error, and when the server is
	// older than rqlite v10.6.0, which does not report codes.
	SQLite *SQLiteErrorCodes `json:"sqlite,omitempty"`
}

// SQLiteErrorCodes holds the result codes SQLite reported for a failed
// statement. See https://www.sqlite.org/rescode.html for their meaning.
type SQLiteErrorCodes struct {
	Code         int `json:"code"`
	ExtendedCode int `json:"extended_code"`
	SystemErrno  int `json:"system_errno,omitempty"`
}

// Error returns the message reported by rqlite, verbatim.
func (e *StatementError) Error() string {
	return e.Message
}

// StatementErrors is the error returned by Query, Write and Request when
// one or more statements in a batch failed. It holds the Err of each
// failed result, in order.
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
	fmt.Fprintf(&sb, "there were %d statement errors", len(errs))
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
