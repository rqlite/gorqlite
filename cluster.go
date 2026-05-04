package gorqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	nurl "net/url"
)

// peer is an internal type representing a single hostname:port.
type peer string

// rqliteCluster captures the discovered cluster topology.
type rqliteCluster struct {
	leader     peer
	otherPeers []peer
	// peerList is the cached leader-then-others retry order.
	peerList []peer
}

// assembleURL composes the full URL for an API call against the
// given peer. It uses url.URL so credentials and path components are
// escaped correctly even when they contain reserved characters.
func (conn *Connection) assembleURL(apiOp apiOperation, p peer) string {
	u := nurl.URL{
		Host: string(p),
	}
	if conn.wantsHTTPS {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}
	if conn.username != "" {
		u.User = nurl.UserPassword(conn.username, conn.password)
	}

	switch apiOp {
	case api_STATUS:
		u.Path = "/status"
	case api_NODES:
		u.Path = "/nodes"
	case api_QUERY:
		u.Path = "/db/query"
	case api_WRITE, api_WRITE_QUEUED:
		u.Path = "/db/execute"
	case api_REQUEST:
		u.Path = "/db/request"
	}

	if apiOp == api_QUERY || apiOp == api_WRITE || apiOp == api_WRITE_QUEUED || apiOp == api_REQUEST {
		// rqlite accepts bare flag-style params like ?timings & ?queue,
		// not key=value. Build the raw query directly to preserve that
		// shape; the level value is always one of a small fixed set so
		// no escaping is needed.
		q := "timings&level=" + consistencyLevelToString[conn.getConsistencyLevel()]
		if conn.getWantsTransactions() {
			q += "&transaction"
		}
		if apiOp == api_WRITE_QUEUED {
			q += "&queue"
		}
		u.RawQuery = q
	}

	out := u.String()
	trace("%s: assembled URL for %s: %s", conn.ID, apiOpName(apiOp), redactURL(out))
	return out
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
		// (6.0+). Decode it lazily and re-parse below.
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

	rc := rqliteCluster{}

	responseBody, err := conn.rqliteApiGet(context.Background(), api_STATUS)
	if err != nil {
		return err
	}
	trace("%s: updateClusterInfo() back from /status OK", conn.ID)

	var status statusResponse
	if err := json.Unmarshal(responseBody, &status); err != nil {
		return fmt.Errorf("could not parse /status response: %w", err)
	}

	leaderRaftAddr, err := parseLeaderRaftAddr(status.Store.Leader)
	if err != nil {
		return err
	}
	trace("%s: leader from /status is %s", conn.ID, leaderRaftAddr)

	// In 5.x, "metadata" maps raft addr -> {api_addr, ...}, so we can
	// translate the raft address we got into the HTTP API address.
	if md, ok := status.Store.Metadata[leaderRaftAddr]; ok {
		if api, ok := md["api_addr"]; ok && api != "" {
			rc.leader = peer(api)
		}
	}

	if rc.leader == "" {
		// 6.0+: fall back to /nodes for the api address.
		trace("metadata didn't carry an api_addr; falling back to /nodes")
		responseBody, err := conn.rqliteApiGet(context.Background(), api_NODES)
		if err != nil {
			return fmt.Errorf("could not determine leader from /nodes call: %w", err)
		}
		trace("%s: updateClusterInfo() back from /nodes OK", conn.ID)

		var nodes nodesResponse
		if err := json.Unmarshal(responseBody, &nodes); err != nil {
			return fmt.Errorf("could not unmarshal /nodes response: %w", err)
		}

		for _, v := range nodes {
			if !v.Reachable {
				continue
			}

			u, err := nurl.Parse(v.APIAddr)
			if err != nil {
				return fmt.Errorf("could not parse api_addr %q: %w", v.APIAddr, err)
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

	trace("%s: cluster: leader=%s otherPeers=%v", conn.ID, rc.leader, rc.otherPeers)

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
