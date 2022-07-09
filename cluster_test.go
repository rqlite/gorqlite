package gorqlite_test

import (
	"testing"

	"github.com/rqlite/gorqlite"
)

func TestInitCluster(t *testing.T) {
	// gorqlite.TraceOn(os.Stderr)
	t.Logf("trying Open: %s\n", testUrl())
	conn, err := gorqlite.Open(testUrl())
	if err != nil {
		t.Logf("--> FAILED")
		t.Fatal(err)
	}

	l, err := conn.Leader()
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	if len(l) < 1 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	p, err := conn.Peers()
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	if len(p) < 1 {
		t.Logf("--> FAILED")
		t.Fail()
	}
}
