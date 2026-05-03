// Package gorqlite provieds a database/sql-like driver for rqlite,
// the distributed consistent sqlite.
//
// Copyright (c)2016 andrew fabbro (andrew@fabbro.org)
//
// See LICENSE.md for license. tl;dr: MIT. Conveniently, the same license as rqlite.
//
// Project home page: https://github.com/raindo308/gorqlite
//
// Learn more about rqlite at: https://github.com/rqlite/rqlite
package gorqlite

// this file contains package-level stuff:
//   consts
//   init()
//   Open, TraceOn(), TraceOff()

import (
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
)

type apiOperation int

const (
	api_QUERY apiOperation = iota
	api_STATUS
	api_WRITE
	api_WRITE_QUEUED
	api_NODES
	api_REQUEST
)

func init() {
	traceOut.Store(traceWriter{Writer: io.Discard})
}

// Open creates and returns a "connection" to rqlite, using
// the default HTTP client.
//
// Since rqlite is stateless, there is no actual connection.
// Open() creates and initializes a gorqlite Connection type,
// which represents various config information.
//
// The URL should be in a form like this:
//
//	http://localhost:4001
//
//	http://     default, no auth, localhost:4001
//	https://    default, no auth, localhost:4001, using https
//
//	http://localhost:1234
//	http://mary:secret2@localhost:1234
//
//	https://mary:secret2@somewhere.example.com:1234
//	https://mary:secret2@somewhere.example.com // will use 4001
func Open(connURL string) (*Connection, error) {
	return OpenWithClient(connURL, DefaultHTTPClient)
}

// OpenWithClient creates and returns a "connection" to rqlite,
// and uses the given HTTP client for all connections to rqlite.
// This allows clients to have complete conntrol over the HTTP
// communications between this client and the rqlite system.
func OpenWithClient(connURL string, client *http.Client) (*Connection, error) {
	conn := &Connection{}

	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	conn.ID = fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	trace("%s: Open() called for url: %s", conn.ID, redactURL(connURL))

	if err := conn.initConnection(connURL, client); err != nil {
		return nil, err
	}

	if !conn.disableClusterDiscovery {
		// call updateClusterInfo() to re-populate the cluster and discover peers
		// also tests the user's default
		if err := conn.updateClusterInfo(); err != nil {
			return nil, err
		}
	}

	return conn, nil
}

// traceWriter wraps an io.Writer so we can store it in an atomic.Value
// (which requires consistent concrete types across stores).
type traceWriter struct{ io.Writer }

// traceOut is read on every trace() call but only written by TraceOn /
// TraceOff. atomic.Value lets us swap the writer without locking the
// hot path, and lets concurrent callers read it without races.
var traceOut atomic.Value // traceWriter

// wantsTrace is non-zero when tracing is enabled. We use a uint32 with
// atomic Load/Store rather than atomic.Bool to keep this package
// usable on Go versions older than 1.19.
var wantsTrace uint32

// trace adds a message to the trace output.
//
// Don't put a \n in your Sprintf pattern because trace() adds one.
func trace(pattern string, args ...interface{}) {
	if atomic.LoadUint32(&wantsTrace) == 0 {
		return
	}

	// make sure there is one and only one newline
	nlPattern := strings.TrimSpace(pattern) + "\n"
	msg := fmt.Sprintf(nlPattern, args...)
	if w, ok := traceOut.Load().(traceWriter); ok && w.Writer != nil {
		w.Write([]byte(msg))
	}
}

// TraceOn turns on tracing output to the io.Writer of your choice.
//
// Trace output is very detailed and verbose, as you might expect.
//
// Normally, you should run with tracing off, as it makes absolutely
// no concession to performance and is intended for debugging/dev use.
func TraceOn(w io.Writer) {
	traceOut.Store(traceWriter{Writer: w})
	atomic.StoreUint32(&wantsTrace, 1)
}

// TraceOff turns off tracing output. Once you call TraceOff(), no further
// info is sent to the io.Writer, unless it is TraceOn'd again.
func TraceOff() {
	atomic.StoreUint32(&wantsTrace, 0)
	traceOut.Store(traceWriter{Writer: io.Discard})
}
