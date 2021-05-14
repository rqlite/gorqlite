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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

/* *****************************************************************

	type: peer

	this is an internal type to abstact peer info.

	note that hostname is sometimes used for "has this struct been
	inialized" checks.

 * *****************************************************************/

type peer struct {
	hostname string //   hostname or "localhost"
	port     string //   "4001" or port, only ever used as a string
}

func (p *peer) String() string {
	return fmt.Sprintf("%s:%s", p.hostname, p.port)
}

/* *****************************************************************

  type: rqliteCluster

	internal type that abstracts the full cluster state (leader, peers)

 * *****************************************************************/

type rqliteCluster struct {
	leader     peer
	otherPeers []peer
	conn       *Connection
}

/* *****************************************************************

  method: rqliteCluster.makePeerList()

	in the api calls, we'll want to try the leader first, then the other
	peers.  to make looping easy, this function returns a list of peers
	in the order the try them: leader, other peer, other peer, etc.

 * *****************************************************************/

func (rc *rqliteCluster) makePeerList() []peer {
	trace("%s: makePeerList() called", rc.conn.ID)
	var peerList []peer
	peerList = append(peerList, rc.leader)
	for _, p := range rc.otherPeers {
		peerList = append(peerList, p)
	}

	trace("%s: makePeerList() returning this list:", rc.conn.ID)
	for n, v := range peerList {
		trace("%s: makePeerList() peer %d -> %s", rc.conn.ID, n, v.hostname+":"+v.port)
	}

	return peerList
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
	var stringBuffer bytes.Buffer

	if conn.wantsHTTPS == true {
		stringBuffer.WriteString("https")
	} else {
		stringBuffer.WriteString("http")
	}
	stringBuffer.WriteString("://")
	if conn.username != "" && conn.password != "" {
		stringBuffer.WriteString(conn.username)
		stringBuffer.WriteString(":")
		stringBuffer.WriteString(conn.password)
		stringBuffer.WriteString("@")
	}
	stringBuffer.WriteString(p.hostname)
	stringBuffer.WriteString(":")
	stringBuffer.WriteString(p.port)

	switch apiOp {
	case api_STATUS:
		stringBuffer.WriteString("/status")
	case api_NODES:
		stringBuffer.WriteString("/nodes")
	case api_QUERY:
		stringBuffer.WriteString("/db/query")
	case api_WRITE:
		stringBuffer.WriteString("/db/execute")
	}

	if apiOp == api_QUERY || apiOp == api_WRITE {
		stringBuffer.WriteString("?timings&level=")
		stringBuffer.WriteString(consistencyLevelNames[conn.consistencyLevel])
		if conn.wantsTransactions {
			stringBuffer.WriteString("&transaction")
		}
	}

	switch apiOp {
	case api_QUERY:
		trace("%s: assembled URL for an api_QUERY: %s", conn.ID, stringBuffer.String())
	case api_STATUS:
		trace("%s: assembled URL for an api_STATUS: %s", conn.ID, stringBuffer.String())
	case api_NODES:
		trace("%s: assembled URL for an api_NODES: %s", conn.ID, stringBuffer.String())
	case api_WRITE:
		trace("%s: assembled URL for an api_WRITE: %s", conn.ID, stringBuffer.String())
	}

	return stringBuffer.String()
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
				parts := strings.Split(peerHttp.(string), ":")
				rc.leader = peer{parts[0], parts[1]}
			}
		}
	}

	if rc.leader.hostname == "" {
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
			if v.Leader {
				u, err := url.Parse(v.APIAddr)
				if err != nil {
					return errors.New("could not parse API address")
				}
				trace("nodes/ indicates %s as API Addr", u.String())
				var host, port string
				if host, port, err = net.SplitHostPort(u.Host); err != nil {
					return fmt.Errorf("could not split host: %s", err)
				}
				rc.leader = peer{host, port}
			}
		}
	} else {
		trace("leader successfully determined using metadata")
	}

	// dump to trace
	trace("%s: here is my cluster config:", conn.ID)
	trace("%s: leader   : %s", conn.ID, rc.leader.String())
	for n, v := range rc.otherPeers {
		trace("%s: otherPeer #%d: %s", conn.ID, n, v.String())
	}

	// now make it official
	conn.cluster = rc

	return nil
}
