package gorqlite

/*
	this file contains some high-level Connection-oriented stuff
*/

/* *****************************************************************

   imports

 * *****************************************************************/

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
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

var (
	// ErrClosed indicates that client connection was closed
	ErrClosed = errors.New("gorqlite: connection is closed")
	traceOut  io.Writer
)

// defaults to false.  This is used in trace() to quickly
// return if tracing is off, so that we don't do a perhaps
// expensive Sprintf() call only to send it to Discard
var wantsTrace bool

/* *****************************************************************

   type: Connection

 * *****************************************************************/

// Connection provides the connection abstraction.
// Note that since rqlite is stateless, there really is no "connection".
// However, this type holds  information such as the current leader, peers,
// connection string to build URLs, etc.
//
// Connections are assigned a "connection ID" which is a pseudo-UUID
// for connection identification in trace output only.  This helps
// sort out what's going on if you have multiple connections going
// at once.  It's generated using a non-standards-or-anything-else-compliant
// function that uses crypto/rand to generate 16 random bytes.
//
// Note that the Connection objection holds info on all peers, gathered
// at time of Open() from the node specified.
type Connection struct {
	cluster rqliteCluster

	// name           type                default

	username                string           //   username or ""
	password                string           //   username or ""
	consistencyLevel        consistencyLevel //   WEAK
	disableClusterDiscovery bool             //   false unless user states otherwise
	wantsHTTPS              bool             //   false unless connection URL is https
	wantsTransactions       bool             //   true unless user states otherwise
	wantsQueueing           bool             //   perform queued writes

	// variables below this line need to be initialized in Open()

	hasBeenClosed bool   //   false
	ID            string //   generated in init()

	client http.Client
}

// Close will mark the connection as closed. It is safe to be called
// multiple times.
func (conn *Connection) Close() {
	conn.hasBeenClosed = true
	trace("%s: %s", conn.ID, "closing connection")
}

// ConsistencyLevel tells the current consistency level
func (conn *Connection) ConsistencyLevel() (string, error) {
	if conn.hasBeenClosed {
		return "", ErrClosed
	}
	return consistencyLevelToString[conn.consistencyLevel], nil
}

// Leader tells the current leader of the cluster
func (conn *Connection) Leader() (string, error) {
	if conn.hasBeenClosed {
		return "", ErrClosed
	}
	if conn.disableClusterDiscovery {
		return string(conn.cluster.leader), nil
	}
	trace("%s: Leader(), calling updateClusterInfo()", conn.ID)
	err := conn.updateClusterInfo()
	if err != nil {
		trace("%s: Leader() got error from updateClusterInfo(): %s", conn.ID, err.Error())
		return "", err
	} else {
		trace("%s: Leader(), updateClusterInfo() OK", conn.ID)
	}
	return string(conn.cluster.leader), nil
}

// Peers tells the current peers of the cluster
func (conn *Connection) Peers() ([]string, error) {
	if conn.hasBeenClosed {
		var ans []string
		return ans, ErrClosed
	}
	plist := make([]string, 0)

	if conn.disableClusterDiscovery {
		for _, p := range conn.cluster.peerList {
			plist = append(plist, string(p))
		}
		return plist, nil
	}

	trace("%s: Peers(), calling updateClusterInfo()", conn.ID)
	err := conn.updateClusterInfo()
	if err != nil {
		trace("%s: Peers() got error from updateClusterInfo(): %s", conn.ID, err.Error())
		return plist, err
	} else {
		trace("%s: Peers(), updateClusterInfo() OK", conn.ID)
	}
	if conn.cluster.leader != "" {
		plist = append(plist, string(conn.cluster.leader))
	}
	for _, p := range conn.cluster.otherPeers {
		plist = append(plist, string(p))
	}
	return plist, nil
}

func (conn *Connection) SetConsistencyLevel(levelDesired consistencyLevel) error {
	if conn.hasBeenClosed {
		return ErrClosed
	}

	if levelDesired < ConsistencyLevelNone || levelDesired > ConsistencyLevelStrong {
		return fmt.Errorf("unknown consistency level: %d", levelDesired)
	}

	conn.consistencyLevel = levelDesired
	return nil
}

func (conn *Connection) SetExecutionWithTransaction(state bool) error {
	if conn.hasBeenClosed {
		return ErrClosed
	}
	conn.wantsTransactions = state
	return nil
}

// initConnection takes the initial connection URL specified by
// the user, and parses it into a peer.  This peer is assumed to
// be the leader.  The next thing Open() does is updateClusterInfo()
// so the truth will be revealed soon enough.
//
// initConnection() does not talk to rqlite.  It only parses the
// connection URL and prepares the new connection for work.
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
	// do some sanity checks.  You know users.

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

	// specs say Username() is always populated even if empty
	if u.User == nil {
		conn.username = ""
		conn.password = ""
	} else {
		// guaranteed, but could be empty which is ok
		conn.username = u.User.Username()

		// not guaranteed, so test if set
		pass, isset := u.User.Password()
		if isset {
			conn.password = pass
		} else {
			conn.password = ""
		}
	}

	if u.Host == "" {
		conn.cluster.leader = "localhost:4001"
	} else {
		conn.cluster.leader = peer(u.Host)
	}
	conn.cluster.peerList = []peer{conn.cluster.leader}

	// at the moment, the only allowed query is "level=" with
	// the desired consistency level

	// default
	conn.consistencyLevel = ConsistencyLevelWeak

	// parse query params
	query := u.Query()
	if query.Get("level") != "" {
		cl, err := ParseConsistencyLevel(query.Get("level"))
		if err != nil {
			return fmt.Errorf("invalid consistency level: %s %w", query.Get("level"), err)
		}
		conn.consistencyLevel = cl
	}

	conn.disableClusterDiscovery = defaultDisableClusterDiscovery
	if query.Get("disableClusterDiscovery") != "" {
		dpd, err := strconv.ParseBool(query.Get("disableClusterDiscovery"))
		if err != nil {
			return errors.New("invalid disableClusterDiscovery value: " + err.Error())
		}
		conn.disableClusterDiscovery = dpd
	}

	timeout := defaultTimeout
	if query.Get("timeout") != "" {
		customTimeout, err := strconv.Atoi(query.Get("timeout"))
		if err != nil {
			return errors.New("invalid timeout specified: " + err.Error())
		}
		timeout = customTimeout
	}

	// Default transaction state
	conn.wantsTransactions = true

	// Initialize http client for connection
	if httpClient == nil {
		conn.client = http.Client{
			Timeout: time.Second * time.Duration(timeout),
		}
	}

	trace("%s: parseDefaultPeer() is done:", conn.ID)
	if conn.wantsHTTPS {
		trace("%s:    %s -> %s", conn.ID, "wants https?", "yes")
	} else {
		trace("%s:    %s -> %s", conn.ID, "wants https?", "no")
	}
	trace("%s:    %s -> %s", conn.ID, "username", conn.username)
	trace("%s:    %s -> %s", conn.ID, "password", conn.password)
	trace("%s:    %s -> %s", conn.ID, "host", conn.cluster.leader)
	trace("%s:    %s -> %s", conn.ID, "consistencyLevel", consistencyLevelToString[conn.consistencyLevel])
	trace("%s:    %s -> %s", conn.ID, "wantsTransaction", conn.wantsTransactions)

	conn.cluster.conn = conn

	return nil
}
