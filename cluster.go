package gorqlite

/*
	this file holds most of the cluster-related stuff:

	types:
		peer
		rqliteCluster
	Connection methods:
		assembleURL (from a peer)
		updateClusterInfo (does the full cluster discovery via status)
*/

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	nurl "net/url"
	"strings"
)

// peer is an internal type representing a single hostname:port.
type peer string

// rqliteCluster captures the discovered cluster topology.
type rqliteCluster struct {
	leader     peer
	otherPeers []peer
	// cached list of peers starting with leader
	peerList []peer
	conn     *Connection
}

// PeerList lists the peers within a rqlite cluster, leader first.
//
// It returns the cached peer list assembled by updateClusterInfo,
// allowing callers to walk the cluster in retry order without
// rebuilding the list on every API call.
func (rc *rqliteCluster) PeerList() []peer {
	return rc.peerList
}

// assembleURL composes the full URL for an API call against the given
// peer.
//
// e.g.: https://mary:secret2@server1.example.com:1234/db/query?transaction&level=strong
//
// Lives on Connection rather than peer because the credentials and
// consistency level are connection-scoped.
func (conn *Connection) assembleURL(apiOp apiOperation, p peer) string {
	var builder strings.Builder

	if conn.wantsHTTPS {
		builder.WriteString("https")
	} else {
		builder.WriteString("http")
	}
	builder.WriteString("://")
	if conn.username != "" && conn.password != "" {
		builder.WriteString(nurl.PathEscape(conn.username))
		builder.WriteString(":")
		builder.WriteString(nurl.PathEscape(conn.password))
		builder.WriteString("@")
	}
	builder.WriteString(string(p))

	switch apiOp {
	case api_STATUS:
		builder.WriteString("/status")
	case api_NODES:
		builder.WriteString("/nodes")
	case api_QUERY:
		builder.WriteString("/db/query")
	case api_WRITE, api_WRITE_QUEUED:
		builder.WriteString("/db/execute")
	case api_REQUEST:
		builder.WriteString("/db/request")
	}

	if apiOp == api_QUERY || apiOp == api_WRITE || apiOp == api_WRITE_QUEUED || apiOp == api_REQUEST {
		builder.WriteString("?timings&level=")
		builder.WriteString(consistencyLevelToString[conn.getConsistencyLevel()])
		if conn.getWantsTransactions() {
			builder.WriteString("&transaction")
		}
		if apiOp == api_WRITE_QUEUED {
			builder.WriteString("&queue")
		}
	}

	trace("%s: assembled URL for %s: %s", conn.ID, apiOpName(apiOp), redactURL(builder.String()))

	return builder.String()
}

func apiOpName(op apiOperation) string {
	switch op {
	case api_QUERY:
		return "api_QUERY"
	case api_STATUS:
		return "api_STATUS"
	case api_NODES:
		return "api_NODES"
	case api_WRITE:
		return "api_WRITE"
	case api_WRITE_QUEUED:
		return "api_WRITE_QUEUED"
	case api_REQUEST:
		return "api_REQUEST"
	}
	return fmt.Sprintf("apiOperation(%d)", op)
}

// statusResponse maps the subset of GET /status we care about.
type statusResponse struct {
	Store struct {
		// "leader" can be either a string (raft addr, 5.x) or a struct
		// (6.0+). We decode it with json.RawMessage and re-parse below.
		Leader   json.RawMessage              `json:"leader"`
		Metadata map[string]map[string]string `json:"metadata"`
	} `json:"store"`
}

// nodesResponse maps the GET /nodes payload.
type nodesResponse map[string]struct {
	APIAddr   string `json:"api_addr,omitempty"`
	Addr      string `json:"addr,omitempty"`
	Reachable bool   `json:"reachable,omitempty"`
	Leader    bool   `json:"leader"`
}

// updateClusterInfo replaces the connection's cluster info with a
// freshly discovered topology (leader + peers).
//
// Retry/timeout handling lives in rqliteApiCall.
func (conn *Connection) updateClusterInfo() error {
	trace("%s: updateClusterInfo() called", conn.ID)

	rc := rqliteCluster{conn: conn}

	responseBody, err := conn.rqliteApiGet(context.Background(), api_STATUS)
	if err != nil {
		return err
	}
	trace("%s: updateClusterInfo() back from api call OK", conn.ID)

	var status statusResponse
	if err := json.Unmarshal(responseBody, &status); err != nil {
		return fmt.Errorf("could not parse /status response: %w", err)
	}

	// Decode the leader field, which can be either a string (raft
	// address, 5.x) or an object with node_id (6.0+).
	leaderRaftAddr, err := parseLeaderRaftAddr(status.Store.Leader)
	if err != nil {
		return err
	}
	trace("%s: leader from store section is %s", conn.ID, leaderRaftAddr)

	// In 5.x, "metadata" maps raft addr -> {api_addr, ...}, so we can
	// translate the raft address we got into the HTTP API address.
	if md, ok := status.Store.Metadata[leaderRaftAddr]; ok {
		if api, ok := md["api_addr"]; ok && api != "" {
			rc.leader = peer(api)
		}
	}

	if rc.leader == "" {
		// 6.0+: fall back to /nodes for the api address.
		trace("getting leader from metadata failed, trying nodes/")
		responseBody, err := conn.rqliteApiGet(context.Background(), api_NODES)
		if err != nil {
			return errors.New("could not determine leader from API nodes call")
		}
		trace("%s: updateClusterInfo() back from api call OK", conn.ID)

		var nodes nodesResponse
		if err := json.Unmarshal(responseBody, &nodes); err != nil {
			return errors.New("could not unmarshal nodes/ response")
		}

		for _, v := range nodes {
			if !v.Reachable {
				continue
			}

			u, err := nurl.Parse(v.APIAddr)
			if err != nil {
				return errors.New("could not parse API address")
			}

			if v.Leader {
				rc.leader = peer(u.Host)
			} else {
				rc.otherPeers = append(rc.otherPeers, peer(u.Host))
			}
		}
	} else {
		trace("leader successfully determined using metadata")
	}

	rc.peerList = buildPeerList(rc.leader, rc.otherPeers)

	trace("%s: here is my cluster config:", conn.ID)
	trace("%s: leader   : %s", conn.ID, rc.leader)
	for n, v := range rc.otherPeers {
		trace("%s: otherPeer #%d: %s", conn.ID, n, v)
	}

	conn.setCluster(rc)

	return nil
}

// parseLeaderRaftAddr extracts the raft address from the polymorphic
// "leader" field of /status. Older rqlite returns a bare string; newer
// versions return an object with a node_id field.
func parseLeaderRaftAddr(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", errors.New("store is not open")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if s == "" {
			return "", errors.New("store is not open")
		}
		return s, nil
	}
	var obj struct {
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil && obj.NodeID != "" {
		return obj.NodeID, nil
	}
	return "", errors.New("store is not open")
}

// buildPeerList returns leader followed by otherPeers, omitting the
// leader if it's empty.
func buildPeerList(leader peer, others []peer) []peer {
	out := make([]peer, 0, len(others)+1)
	if leader != "" {
		out = append(out, leader)
	}
	out = append(out, others...)
	return out
}
