package gorqlite

/*
	this file contains some high-level Connection-oriented stuff
*/

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	nurl "net/url"
)

const (
	defaultTimeout                 = 10
	defaultDisableClusterDiscovery = false
)

var DefaultHTTPClient = &http.Client{
	Timeout: defaultTimeout * time.Second,
}

// ErrClosed indicates that the connection was closed.
var ErrClosed = errors.New("gorqlite: connection is closed")

// Connection provides the connection abstraction.
//
// Since rqlite is stateless, there is no real connection — this type
// holds the configuration plus the discovered cluster topology
// (current leader, peers, scheme, credentials, etc.).
//
// Connections are assigned a "connection ID" — a pseudo-UUID built
// from 16 crypto/rand bytes — used only for tagging trace output, so
// concurrent connections can be distinguished.
//
// Connection methods are safe to call concurrently from multiple
// goroutines. State changes go through a single mutex; the HTTP
// client is reused as-is so the standard library handles its own
// concurrency.
type Connection struct {
	// mu guards everything mutated after Open: cluster, consistency
	// level, transaction flag, and the closed flag.
	mu      sync.RWMutex
	cluster rqliteCluster

	username                string
	password                string
	consistencyLevel        consistencyLevel
	disableClusterDiscovery bool
	wantsHTTPS              bool
	wantsTransactions       bool

	hasBeenClosed bool
	ID            string

	client *http.Client
}

// Close marks the connection as closed. Safe to call multiple times.
func (conn *Connection) Close() {
	conn.mu.Lock()
	conn.hasBeenClosed = true
	conn.mu.Unlock()
	trace("%s: %s", conn.ID, "closing connection")
}

// isClosed reports whether Close has been called. Read-locked because
// it's hit on every API call.
func (conn *Connection) isClosed() bool {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	return conn.hasBeenClosed
}

func (conn *Connection) getConsistencyLevel() consistencyLevel {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	return conn.consistencyLevel
}

func (conn *Connection) getWantsTransactions() bool {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	return conn.wantsTransactions
}

// setCluster atomically replaces the discovered cluster topology.
func (conn *Connection) setCluster(rc rqliteCluster) {
	conn.mu.Lock()
	conn.cluster = rc
	conn.mu.Unlock()
}

// snapshotPeerList returns a copy of the cached peer list under lock,
// so iteration in rqliteApiCall is race-free even if a concurrent
// updateClusterInfo replaces the cluster mid-iteration.
func (conn *Connection) snapshotPeerList() []peer {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if len(conn.cluster.peerList) == 0 {
		return nil
	}
	out := make([]peer, len(conn.cluster.peerList))
	copy(out, conn.cluster.peerList)
	return out
}

// ConsistencyLevel returns the current consistency level as a string.
func (conn *Connection) ConsistencyLevel() (string, error) {
	if conn.isClosed() {
		return "", ErrClosed
	}
	return consistencyLevelToString[conn.getConsistencyLevel()], nil
}

// Leader returns the current cluster leader's API address.
func (conn *Connection) Leader() (string, error) {
	if conn.isClosed() {
		return "", ErrClosed
	}
	if conn.disableClusterDiscovery {
		conn.mu.RLock()
		leader := string(conn.cluster.leader)
		conn.mu.RUnlock()
		return leader, nil
	}
	trace("%s: Leader(), calling updateClusterInfo()", conn.ID)
	if err := conn.updateClusterInfo(); err != nil {
		trace("%s: Leader() got error from updateClusterInfo(): %s", conn.ID, err.Error())
		return "", err
	}
	trace("%s: Leader(), updateClusterInfo() OK", conn.ID)
	conn.mu.RLock()
	leader := string(conn.cluster.leader)
	conn.mu.RUnlock()
	return leader, nil
}

// Peers returns the current cluster peers, leader first.
func (conn *Connection) Peers() ([]string, error) {
	if conn.isClosed() {
		return nil, ErrClosed
	}
	if conn.disableClusterDiscovery {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		plist := make([]string, 0, len(conn.cluster.peerList))
		for _, p := range conn.cluster.peerList {
			plist = append(plist, string(p))
		}
		return plist, nil
	}

	trace("%s: Peers(), calling updateClusterInfo()", conn.ID)
	if err := conn.updateClusterInfo(); err != nil {
		trace("%s: Peers() got error from updateClusterInfo(): %s", conn.ID, err.Error())
		return nil, err
	}
	trace("%s: Peers(), updateClusterInfo() OK", conn.ID)

	conn.mu.RLock()
	defer conn.mu.RUnlock()
	plist := make([]string, 0, 1+len(conn.cluster.otherPeers))
	if conn.cluster.leader != "" {
		plist = append(plist, string(conn.cluster.leader))
	}
	for _, p := range conn.cluster.otherPeers {
		plist = append(plist, string(p))
	}
	return plist, nil
}

// SetConsistencyLevel sets the consistency level used for future
// queries on this connection.
func (conn *Connection) SetConsistencyLevel(levelDesired consistencyLevel) error {
	if conn.isClosed() {
		return ErrClosed
	}

	if levelDesired < ConsistencyLevelNone || levelDesired > ConsistencyLevelStrong {
		return fmt.Errorf("unknown consistency level: %d", levelDesired)
	}

	conn.mu.Lock()
	conn.consistencyLevel = levelDesired
	conn.mu.Unlock()
	return nil
}

// SetExecutionWithTransaction toggles whether multi-statement
// queries/writes are wrapped in a single rqlite transaction.
func (conn *Connection) SetExecutionWithTransaction(state bool) error {
	if conn.isClosed() {
		return ErrClosed
	}
	conn.mu.Lock()
	conn.wantsTransactions = state
	conn.mu.Unlock()
	return nil
}

// initConnection takes the initial connection URL specified by
// the user, and a HTTP client. The URL is parsed to determine
// the peer's addresss. This peer is assumed to be the leader.
// The next thing Open() does is updateClusterInfo()
// so the truth will be revealed soon enough.
//
// initConnection() does not talk to rqlite.  It only parses the
// connection URL and prepares the new connection for work. If
// the HTTP client is nil, then the default client is used.
//
// URL format:
//
//	http[s]://${USER}:${PASSWORD}@${HOSTNAME}:${PORT}/db?[OPTIONS]
//
// Examples:
//
//	https://mary:secret2@localhost:4001/db
//	https://mary:secret2@server1.example.com:4001/db?level=none
//	https://mary:secret2@server2.example.com:4001/db?level=weak
//	https://mary:secret2@localhost:2265/db?level=strong
//
// to use default connection to localhost:4001 with no auth:
//
//	http://
//	https://
//
// guaranteed map fields - will be set to "" if not specified
//
//	field name                  default if not specified
//
//	username                    ""
//	password                    ""
//	hostname                    "localhost"
//	port                        "4001"
//	consistencyLevel            "weak"
func (conn *Connection) initConnection(url string, httpClient *http.Client) error {
	if len(url) < 7 {
		return errors.New("url specified is impossibly short")
	}

	if !strings.HasPrefix(url, "http") {
		return errors.New("url does not start with 'http'")
	}

	u, err := nurl.Parse(url)
	if err != nil {
		return err
	}
	trace("%s: net.url.Parse() OK", conn.ID)

	if u.Scheme == "https" {
		conn.wantsHTTPS = true
	}

	if u.User != nil {
		conn.username = u.User.Username()
		if pass, isset := u.User.Password(); isset {
			conn.password = pass
		}
	}

	if u.Host == "" {
		conn.cluster.leader = "localhost:4001"
	} else {
		conn.cluster.leader = peer(u.Host)
	}
	conn.cluster.peerList = []peer{conn.cluster.leader}

	conn.consistencyLevel = ConsistencyLevelWeak

	query := u.Query()
	if level := query.Get("level"); level != "" {
		cl, err := ParseConsistencyLevel(level)
		if err != nil {
			return fmt.Errorf("invalid consistency level: %s %w", level, err)
		}
		conn.consistencyLevel = cl
	}

	conn.disableClusterDiscovery = defaultDisableClusterDiscovery
	if v := query.Get("disableClusterDiscovery"); v != "" {
		dpd, err := strconv.ParseBool(v)
		if err != nil {
			return errors.New("invalid disableClusterDiscovery value: " + err.Error())
		}
		conn.disableClusterDiscovery = dpd
	}

	timeout := defaultTimeout
	if v := query.Get("timeout"); v != "" {
		customTimeout, err := strconv.Atoi(v)
		if err != nil {
			return errors.New("invalid timeout specified: " + err.Error())
		}
		timeout = customTimeout
	}

	conn.wantsTransactions = true

	conn.client = httpClient
	if conn.client == nil {
		conn.client = &http.Client{
			Timeout: time.Second * time.Duration(timeout),
		}
	}

	trace("%s: parseDefaultPeer() is done:", conn.ID)
	trace("%s:    wants https? -> %t", conn.ID, conn.wantsHTTPS)
	trace("%s:    username -> %s", conn.ID, conn.username)
	trace("%s:    password -> %s", conn.ID, redactedPassword(conn.password))
	trace("%s:    host -> %s", conn.ID, conn.cluster.leader)
	trace("%s:    consistencyLevel -> %s", conn.ID, consistencyLevelToString[conn.consistencyLevel])
	trace("%s:    wantsTransaction -> %t", conn.ID, conn.wantsTransactions)

	conn.cluster.conn = conn

	return nil
}

// redactedPassword returns a fixed mask if password is set, or empty
// otherwise. Used so trace output never reveals the real value.
func redactedPassword(p string) string {
	if p == "" {
		return ""
	}
	return "[redacted]"
}
