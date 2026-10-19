// Package stdlib provides a compatibility layer from gorqlite to database/sql.
package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/rqlite/gorqlite"
)

func init() {
	sql.Register("rqlite", &Driver{})
}

type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	conn, err := gorqlite.Open(name)
	if err != nil {
		return nil, err
	}
	return &Conn{conn}, nil
}

type Conn struct {
	*gorqlite.Connection
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{Stmt: query, Conn: c}, nil
}

func (c *Conn) Close() error {
	c.Connection.Close()
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return &Tx{}, nil
}

type Tx struct{}

func (tx *Tx) Commit() error {
	// no-op
	return nil
}

func (tx *Tx) Rollback() error {
	// no-op
	return nil
}

type Stmt struct {
	Stmt string
	Conn *Conn
}

// these aren't checked automatically anywhere else, so we check them here
var _ driver.StmtExecContext = (*Stmt)(nil)
var _ driver.StmtQueryContext = (*Stmt)(nil)

func (s *Stmt) Close() error {
	return nil
}

func (s *Stmt) NumInput() int {
	return -1
}

// statement binds positional driver values to the SQL text.
func (s *Stmt) statement(args []driver.Value) gorqlite.ParameterizedStatement {
	a := make([]interface{}, len(args))
	for i, v := range args {
		a[i] = v
	}
	return gorqlite.ParameterizedStatement{Query: s.Stmt, Arguments: a}
}

// namedStatement binds driver.NamedValues to the SQL text. rqlite
// only supports positional parameters, so a named value is an error.
func (s *Stmt) namedStatement(args []driver.NamedValue) (gorqlite.ParameterizedStatement, error) {
	a := make([]interface{}, len(args))
	for _, v := range args {
		if v.Name != "" {
			return gorqlite.ParameterizedStatement{}, fmt.Errorf("rqlite: driver does not support named parameters, but got one with name %q in statement %q", v.Name, s.Stmt)
		}
		a[v.Ordinal-1] = v.Value
	}
	return gorqlite.ParameterizedStatement{Query: s.Stmt, Arguments: a}, nil
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	wr, err := s.Conn.WriteOneParameterized(s.statement(args))
	return &Result{wr}, err
}

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := s.namedStatement(args)
	if err != nil {
		return nil, err
	}
	wr, err := s.Conn.WriteOneParameterizedContext(ctx, stmt)
	return &Result{wr}, err
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	qr, err := s.Conn.QueryOneParameterized(s.statement(args))
	return &Rows{qr}, err
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := s.namedStatement(args)
	if err != nil {
		return nil, err
	}
	qr, err := s.Conn.QueryOneParameterizedContext(ctx, stmt)
	return &Rows{qr}, err
}

type Result struct {
	gorqlite.WriteResult
}

func (r *Result) LastInsertId() (int64, error) {
	return r.WriteResult.LastInsertID, r.WriteResult.Err
}

func (r *Result) RowsAffected() (int64, error) {
	return r.WriteResult.RowsAffected, r.WriteResult.Err
}

type Rows struct {
	gorqlite.QueryResult
}

func (r *Rows) Columns() []string {
	return r.QueryResult.Columns()
}

// Close releases iteration resources. Errors that occurred while the
// underlying gorqlite query ran were already returned by db.Query, so
// Close itself reports nothing — matching the database/sql convention
// of treating Close() errors as "best effort, advisory only" for
// drivers that don't actually hold streamed state open.
func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	ok := r.QueryResult.Next()
	if !ok {
		return io.EOF
	}
	// in Next, we are copying the values into the destination
	// slice, not directly scanning them
	slice, err := r.QueryResult.Slice()
	if err != nil {
		return err
	}
	for i, v := range slice {
		dest[i] = v
	}
	return nil
}
