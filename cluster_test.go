package gorqlite

import "testing"

import "os"

func TestProcessInfoRespone(t *testing.T) {

	testInfoReponse := `{"build":{"branch":"master","build_time":"2021-08-05T21:24:10-0400","commit":"7103d425c8a50a24fa81812d85c45d5fc26b15d7","version":"v6.1.0"},"cluster":{"addr":"10.1.101.4:8202","api_addr":"10.1.101.4:8201","https":"false","timeout":"10s"},"http":{"addr":"10.1.101.4:8201","auth":"disabled"},"node":{"start_time":"2021-08-12T21:44:43.505297916Z","uptime":"218h29m11.611649608s"},"runtime":{"GOARCH":"amd64","GOMAXPROCS":64,"GOOS":"linux","num_cpu":64,"num_goroutine":23,"version":"go1.15"},"store":{"addr":"10.1.101.4:8202","apply_timeout":"10s","db_applied_index":147993,"db_conf":{"Memory":false},"dir":"/home/srv-demov3-210/rqlite-node","dir_size":61149182,"election_timeout":"1s","fsm_index":148002,"heartbeat_timeout":"1s","leader":{"addr":"10.4.101.4:8202","node_id":"ld8-001"},"node_id":"sv3-001","nodes":[{"id":"ch1-001","addr":"10.14.101.4:8202","suffrage":"Nonvoter"},{"id":"dc1-001","addr":"10.2.101.4:8202","suffrage":"Voter"},{"id":"fr5-001","addr":"10.7.101.4:8202","suffrage":"Voter"},{"id":"ld8-001","addr":"10.4.101.4:8202","suffrage":"Voter"},{"id":"sv3-001","addr":"10.1.101.4:8202","suffrage":"Voter"},{"id":"sy1-001","addr":"10.5.101.4:8202","suffrage":"Voter"}],"raft":{"applied_index":148002,"commit_index":148002,"fsm_pending":0,"last_contact":"38.841182ms","last_log_index":148002,"last_log_term":18244,"last_snapshot_index":144544,"last_snapshot_term":8955,"latest_configuration":"[{Suffrage:Voter ID:sv3-001 Address:10.1.101.4:8202} {Suffrage:Voter ID:dc1-001 Address:10.2.101.4:8202} {Suffrage:Voter ID:ld8-001 Address:10.4.101.4:8202} {Suffrage:Voter ID:sy1-001 Address:10.5.101.4:8202} {Suffrage:Voter ID:fr5-001 Address:10.7.101.4:8202} {Suffrage:Nonvoter ID:ch1-001 Address:10.14.101.4:8202}]","latest_configuration_index":0,"log_size":25169920,"num_peers":4,"protocol_version":3,"protocol_version_max":3,"protocol_version_min":0,"snapshot_version_max":1,"snapshot_version_min":0,"state":"Follower","term":18244},"request_marshaler":{"compression_batch":5,"compression_size":150,"force_compression":false},"snapshot_interval":30000000000,"snapshot_threshold":8192,"sqlite3":{"compile_options":["COMPILER=gcc-7.5.0","DEFAULT_WAL_SYNCHRONOUS=1","ENABLE_DBSTAT_VTAB","ENABLE_FTS3","ENABLE_FTS3_PARENTHESIS","ENABLE_JSON1","ENABLE_RTREE","ENABLE_UPDATE_DELETE_LIMIT","OMIT_DEPRECATED","OMIT_SHARED_CACHE","SYSTEM_MALLOC","THREADSAFE=1"],"conn_pool_stats":{"ro":{"max_open_connections":0,"open_connections":2,"in_use":0,"idle":2,"wait_count":0,"wait_duration":0,"max_idle_closed":1,"max_idle_time_closed":0,"max_lifetime_closed":0},"rw":{"max_open_connections":0,"open_connections":1,"in_use":0,"idle":1,"wait_count":0,"wait_duration":0,"max_idle_closed":0,"max_idle_time_closed":0,"max_lifetime_closed":0}},"db_size":20099072,"path":"/home/srv-demov3-210/rqlite-node/db.sqlite","ro_dsn":"file:/home/srv-demov3-210/rqlite-node/db.sqlite?mode=ro","rw_dsn":"file:/home/srv-demov3-210/rqlite-node/db.sqlite","size":20099072,"version":"3.36.0"},"trailing_logs":10240}}`

	testConn := Connection{}
	var rc rqliteCluster
	err := testConn.processClusterInfoBody([]byte(testInfoReponse), &rc)
	if err != nil || len(rc.otherPeers) == 0 {
		t.Fatal(err)
	}
}

func TestInitCluster(t *testing.T) {

	TraceOn(os.Stderr)
	t.Logf("trying Open: %s\n", testUrl())
	conn, err := Open(testUrl())
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
