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

/* *****************************************************************

   imports

 * *****************************************************************/

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

/* *****************************************************************

	type: peer

	this is an internal type to abstract peer info, actually just
	represent a single hostname:port

 * *****************************************************************/

type peer string

/* *****************************************************************

  type: rqliteCluster

	internal type that abstracts the full cluster state (leader, peers)

 * *****************************************************************/

type rqliteCluster struct {
	leader     peer
	otherPeers []peer
	// cached list of peers starting with leader
	peerList []peer
	conn     *Connection
}

/* *****************************************************************

  method: rqliteCluster.PeerList()

	in the api calls, we'll want to try the leader first, then the other
	peers.  to make looping easy, this function returns a list of peers
	in the order the try them: leader, other peer, other peer, etc.
	since the peer list might change only during updateClusterInfo(),
	we keep it cached

 * *****************************************************************/

func (rc *rqliteCluster) PeerList() []peer {
	return rc.peerList
}

/* *****************************************************************

	method: Connection.assembleURL()

	tell it what peer to talk to and what kind of API operation you're
	making, and it will return the full URL, from start to finish.
	e.g.:

	https://mary:secret2@server1.example.com:1234/db/query?transaction&level=strong

	note: this func needs to live at the Connection level because the
	Connection holds the username, password, consistencyLevel, etc.

 * *****************************************************************/

func (conn *Connection) assembleURL(apiOp apiOperation, p peer) string {
	var builder strings.Builder

	if conn.wantsHTTPS == true {
		builder.WriteString("https")
	} else {
		builder.WriteString("http")
	}
	builder.WriteString("://")
	if conn.username != "" && conn.password != "" {
		builder.WriteString(conn.username)
		builder.WriteString(":")
		builder.WriteString(conn.password)
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
	case api_WRITE:
		builder.WriteString("/db/execute")
	}

	if apiOp == api_QUERY || apiOp == api_WRITE {
		builder.WriteString("?timings&level=")
		builder.WriteString(consistencyLevelNames[conn.consistencyLevel])
		if conn.wantsTransactions {
			builder.WriteString("&transaction")
		}
	}

	switch apiOp {
	case api_QUERY:
		trace("%s: assembled URL for an api_QUERY: %s", conn.ID, builder.String())
	case api_STATUS:
		trace("%s: assembled URL for an api_STATUS: %s", conn.ID, builder.String())
	case api_NODES:
		trace("%s: assembled URL for an api_NODES: %s", conn.ID, builder.String())
	case api_WRITE:
		trace("%s: assembled URL for an api_WRITE: %s", conn.ID, builder.String())
	}

	return builder.String()
}

/* *****************************************************************

	method: Connection.updateClusterInfo()

	upon invocation, updateClusterInfo() completely erases and refreshes
	the Connection's cluster info, replacing its rqliteCluster object
	with current info.

	the web heavy lifting (retrying, etc.) is done in rqliteApiGet()

 * *****************************************************************/

func (conn *Connection) updateClusterInfo() error {
	trace("%s: updateClusterInfo() called", conn.ID)

	// start with a fresh new cluster
	var rc rqliteCluster
	rc.conn = conn

	responseBody, err := conn.rqliteApiGet(api_STATUS)
	if err != nil {
		return err
	}
	trace("%s: updateClusterInfo() back from api call OK", conn.ID)

	sections := make(map[string]interface{})
	err = json.Unmarshal(responseBody, &sections)
	if err != nil {
		return err
	}
	sMap := sections["store"].(map[string]interface{})
	leaderMap, ok := sMap["leader"].(map[string]interface{})
	var leaderRaftAddr string
	if ok {
		leaderRaftAddr = leaderMap["node_id"].(string)
	} else {
		leaderRaftAddr = sMap["leader"].(string)
	}
	trace("%s: leader from store section is %s", conn.ID, leaderRaftAddr)

	// In 5.x and earlier, "metadata" is available
	// leader in this case is the RAFT address
	// we want the HTTP address, so we'll use this as
	// a key as we sift through APIPeers
	apiPeers, ok := sMap["metadata"].(map[string]interface{})
	if !ok {
		apiPeers = map[string]interface{}{}
	}

	if apiAddrMap, ok := apiPeers[leaderRaftAddr]; ok {
		if _httpAddr, ok := apiAddrMap.(map[string]interface{}); ok {
			if peerHttp, ok := _httpAddr["api_addr"]; ok {
				rc.leader = peer(peerHttp.(string))
			}
		}
	}

	if rc.leader == "" {
		// nodes/ API is available in 6.0+
		trace("getting leader from metadata failed, trying nodes/")
		responseBody, err := conn.rqliteApiGet(api_NODES)
		if err != nil {
			return errors.New("could not determine leader from API nodes call")
		}
		trace("%s: updateClusterInfo() back from api call OK", conn.ID)

		nodes := make(map[string]struct {
			APIAddr   string `json:"api_addr,omitempty"`
			Addr      string `json:"addr,omitempty"`
			Reachable bool   `json:"reachable,omitempty"`
			Leader    bool   `json:"leader"`
		})
		err = json.Unmarshal(responseBody, &nodes)
		if err != nil {
			return errors.New("could not unmarshal nodes/ response")
		}

		for _, v := range nodes {
			if !v.Reachable {
				continue
			}

			u, err := url.Parse(v.APIAddr)
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

	rc.peerList = make([]peer, len(rc.otherPeers)+1)
	if rc.leader != "" {
		rc.peerList = append(rc.peerList, rc.leader)
	}
	for _, p := range rc.otherPeers {
		rc.peerList = append(rc.peerList, p)
	}

	// dump to trace
	trace("%s: here is my cluster config:", conn.ID)
	trace("%s: leader   : %s", conn.ID, rc.leader)
	for n, v := range rc.otherPeers {
		trace("%s: otherPeer #%d: %s", conn.ID, n, v)
	}

	// now make it official
	conn.cluster = rc

	return nil
}
