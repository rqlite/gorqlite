package gorqlite_test

import (
	"testing"
)

func TestLeader(t *testing.T) {
	leaders, err := globalConnection.Leader()
	if err != nil {
		t.Errorf("failed to get leader: %v", err.Error())
	}

	if len(leaders) < 1 {
		t.Errorf("expected leaders to be at least 1, got %d", len(leaders))
	}
}

func TestPeers(t *testing.T) {
	peers, err := globalConnection.Peers()
	if err != nil {
		t.Errorf("failed to get peers: %v", err.Error())
	}

	if len(peers) < 1 {
		t.Errorf("expected peers to be at least 1, got %d", len(peers))
	}
}
