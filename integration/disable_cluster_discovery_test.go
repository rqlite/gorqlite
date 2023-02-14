package integration

import (
	_ "embed"
	"io/ioutil"
	"testing"

	"github.com/rqlite/gorqlite"
)

func TestDisableClusterDiscovery(t *testing.T) {
	clusterStatus, err := ioutil.ReadFile("assets/three_node_cluster_status.json")
	if err != nil {
		t.Errorf("failed to read cluster statu json files: %v", err)
		return
	}

	clusterNodes, err := ioutil.ReadFile("assets/three_node_cluster_nodes.json")
	if err != nil {
		t.Errorf("failed to read cluster nodes json files: %v", err)
		return
	}

	mockServer := &MockServer{
		Status: clusterStatus,
		Nodes:  clusterNodes,
	}
	mockServer.Start()
	defer mockServer.Stop()

	if err := mockServer.WaitForReady(); err != nil {
		t.Errorf("mock server failed to start: %v", err)
		return
	}

	conn, err := gorqlite.Open("http://localhost:14001?disableClusterDiscovery=true")
	if err != nil {
		t.Errorf("failed to open connection: %v", err)
		return
	}

	leader, err := conn.Leader()
	if err != nil {
		t.Errorf("failed to get leader: %v", err)
		return
	}
	if leader != "localhost:14001" {
		t.Errorf("leader should be localhost:14001, but is %s", leader)
	}

	peers, err := conn.Peers()
	if err != nil {
		t.Errorf("failed to get peers: %v", err)
		return
	}
	if len(peers) != 1 {
		t.Errorf("expected 1 peer, but got %d", len(peers))
	}
	if peers[0] != "localhost:14001" {
		t.Errorf("peer should be localhost:14001, but is %s", peers[0])
	}
}
