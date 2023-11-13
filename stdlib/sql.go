// Package stdlib provides a compatability layer from gorqlite to database/sql.
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

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	a := make([]interface{}, len(args))
	for i, v := range args {
		a[i] = v
	}
	stmt := gorqlite.ParameterizedStatement{Query: s.Stmt, Arguments: a}
	wr, err := s.Conn.WriteOneParameterized(stmt)
	if err != nil {
		return &Result{wr}, err
	}
	return &Result{wr}, nil
}

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	a := make([]interface{}, len(args))
	for _, v := range args {
		if v.Name != "" {
			return nil, fmt.Errorf("rqlite: driver does not support named parameters, but got one with name %q in statement %q", v.Name, s.Stmt)
		}
		a[v.Ordinal-1] = v.Value
	}
	stmt := gorqlite.ParameterizedStatement{Query: s.Stmt, Arguments: a}
	wr, err := s.Conn.WriteOneParameterizedContext(ctx, stmt)
	if err != nil {
		return &Result{wr}, err
	}
	return &Result{wr}, nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	a := make([]interface{}, len(args))
	for i, v := range args {
		a[i] = v
	}
	stmt := gorqlite.ParameterizedStatement{Query: s.Stmt, Arguments: a}
	qr, err := s.Conn.QueryOneParameterized(stmt)
	if err != nil {
		return &Rows{qr}, err
	}
	return &Rows{qr}, nil
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	a := make([]interface{}, len(args))
	for _, v := range args {
		if v.Name != "" {
			return nil, fmt.Errorf("rqlite: driver does not support named parameters, but got one with name %q in statement %q", v.Name, s.Stmt)
		}
		a[v.Ordinal-1] = v.Value
	}
	stmt := gorqlite.ParameterizedStatement{Query: s.Stmt, Arguments: a}
	qr, err := s.Conn.QueryOneParameterizedContext(ctx, stmt)
	if err != nil {
		return &Rows{qr}, err
	}
	return &Rows{qr}, nil
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

func (r *Rows) Close() error {
	return r.Err
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
