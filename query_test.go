package gorqlite

import "testing"

func TestQueryOne (t *testing.T) {
	var wr WriteResult
	var qr QueryResult
	var wResults []WriteResult
	var qResults []QueryResult
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if ( err != nil ) {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName())
	if ( err != nil ) {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying WriteOne CREATE")
	wr, err = conn.WriteOne("CREATE TABLE " + testTableName() + " (id integer, name text)")
	if ( err != nil ) {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying Write INSERT")
	s := make([]string,0)
	s = append(s,"INSERT INTO " + testTableName() + " (id, name) VALUES ( 1, 'Romulan' )")
	s = append(s,"INSERT INTO " + testTableName() + " (id, name) VALUES ( 2, 'Vulcan' )")
	s = append(s,"INSERT INTO " + testTableName() + " (id, name) VALUES ( 3, 'Klingon' )")
	s = append(s,"INSERT INTO " + testTableName() + " (id, name) VALUES ( 4, 'Ferengi' )")
	s = append(s,"INSERT INTO " + testTableName() + " (id, name) VALUES ( 5, 'Cardassian' )")
	wResults, err = conn.Write(s)
	if ( err != nil ) {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying QueryOne")
	qr, err = conn.QueryOne("SELECT name FROM " + testTableName() + " WHERE id > 3")
	if ( err != nil ) {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Next()")
	na := qr.Next()
	if ( na != true ) {
		t.Logf("--> FAILED")
		t.Fail()
	}
		
	t.Logf("trying Map()")
	r, err := qr.Map()
	if ( err != nil ) {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if ( r["name"].(string) != "Ferengi" ) {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Scan(), also float64->int64 in Scan()")
	var id int64
	var name string
	err = qr.Scan(&id,&name)
	if ( err == nil ) {
		t.Logf("--> FAILED (%s)",err.Error())
		t.Fail()
	}
	err = qr.Scan(&name)
	if ( err != nil ) {
		t.Logf("--> FAILED (%s)",err.Error())
		t.Fail()
	}
	if ( name != "Ferengi" ) {
		t.Logf("--> FAILED, name should be 'Ferengi' but it's '%s'",name)
		t.Fail()
	}
	qr.Next()
	err = qr.Scan(&name)
	if ( name != "Cardassian" ) {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if ( err != nil ) {
		t.Logf("--> FAILED")
		t.Fail()
	}
	
	t.Logf("trying Close")
	conn.Close()

	t.Logf("trying WriteOne after Close")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if ( err == nil ) {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = wr

	t.Logf("trying Write after Close")
	t1 := make([]string,0)
	t1 = append(t1,"DROP TABLE IF EXISTS " + testTableName() + "")
	t1 = append(t1,"DROP TABLE IF EXISTS " + testTableName() + "")
	wResults, err = conn.Write(t1)
	if ( err == nil ) {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = wResults

	t.Logf("trying QueryOne after Close")
	qr, err = conn.QueryOne("SELECT id FROM " + testTableName() + "")
	if ( err == nil ) {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = qr

	t.Logf("trying Query after Close")
	t2 := make([]string,0)
	t2 = append(t2,"SELECT id FROM " + testTableName() + "")
	t2 = append(t2,"SELECT name FROM " + testTableName() + "")
	t2 = append(t2,"SELECT id,name FROM " + testTableName() + "")
	qResults, err = conn.Query(t2)
	if ( err == nil ) {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = qResults

	
}

