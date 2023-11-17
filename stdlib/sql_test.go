package stdlib

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

var globalDB *sql.DB

func TestMain(m *testing.M) {
	db, err := sql.Open("rqlite", testUrl())
	if err != nil {
		log.Fatalf("opening database: %v", err)
	}
	globalDB = db

	exitCode := m.Run()

	err = db.Close()
	if err != nil {
		log.Fatalf("closing database: %v", err)
	}

	os.Exit(exitCode)
}

func testUrl() string {
	url := os.Getenv("GORQLITE_TEST_URL")
	if url == "" {
		url = "http://"
	}
	return url
}

func testTableName() string {
	tableName := os.Getenv("GORQLITE_TEST_TABLE_STDLIB")
	if tableName == "" {
		tableName = "gorqlite_test_stdlib"
	}
	return tableName
}

type payday struct {
	Met string
}

func (p *payday) Scan(src interface{}) error {
	str, ok := src.(string)
	if !ok {
		return fmt.Errorf("expected source of payday to be of type string, not %T", src)
	}
	return json.Unmarshal([]byte(str), p)
}

func TestTable(t *testing.T) {
	// give an extra second for time diff between local and rqlite
	started := time.Now().Add(-time.Second)

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	t.Logf("trying Exec CREATE TABLE")

	_, err := globalDB.Exec("CREATE TABLE " + testTableName() + " (id INTEGER, name TEXT, wallet REAL, bankrupt INTEGER, payload BLOB, ts DATETIME)")
	if err != nil {
		t.Errorf("create: %v", err)
	}

	t.Cleanup(func() {
		_, err := globalDB.Exec("DROP TABLE " + testTableName())
		if err != nil {
			t.Errorf("dropping table: %v", err)
		}
	})

	t.Run("Exec INSERT", func(t *testing.T) {
		_, err = globalDB.Exec("INSERT INTO " + testTableName() + " (id, name, wallet, bankrupt, payload, ts) VALUES ( 1, 'Romulan', 123.456, 0, '{\"met\":\"" + met + "\"}', " + fmt.Sprint(time.Now().Unix()) + " )")
		if err != nil {
			t.Errorf("insert: %v", err)
		}
		_, err = globalDB.Exec("INSERT INTO " + testTableName() + " (id, name, wallet, bankrupt, payload, ts) VALUES ( 2, 'Vulcan', 123.456, 0, '{\"met\":\"" + met + "\"}', " + fmt.Sprint(time.Now().Unix()) + " )")
		if err != nil {
			t.Errorf("insert: %v", err)
		}
		_, err = globalDB.Exec("INSERT INTO " + testTableName() + " (id, name, wallet, bankrupt, payload, ts) VALUES ( 3, 'Klingon', 123.456, 1, '{\"met\":\"" + met + "\"}', " + fmt.Sprint(time.Now().Unix()) + " )")
		if err != nil {
			t.Errorf("insert: %v", err)
		}
		_, err = globalDB.Exec("INSERT INTO " + testTableName() + " (id, name, wallet, bankrupt, payload, ts) VALUES ( 4, 'Ferengi', 123.456, 0, '{\"met\":\"" + met + "\"}', " + fmt.Sprint(time.Now().Unix()) + " )")
		if err != nil {
			t.Errorf("insert: %v", err)
		}
		_, err = globalDB.Exec("INSERT INTO " + testTableName() + " (id, name, wallet, bankrupt, payload, ts) VALUES ( 5, 'Cardassian', 123.456, 1, '{\"met\":\"" + met + "\"}', " + met + " )")
		if err != nil {
			t.Errorf("insert: %v", err)
		}
	})

	t.Run("Query SELECT", func(t *testing.T) {
		rows, err := globalDB.Query("SELECT name, ts, wallet, bankrupt, payload FROM " + testTableName() + " WHERE id > 3")
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			t.Errorf("getting columns: %v", err)
		}
		if len(columns) != 5 {
			t.Errorf("expected 5 columns, got %v", len(columns))
		} else {
			expect := []string{"name", "ts", "wallet", "bankrupt", "payload"}
			for i, c := range columns {
				if c != expect[i] {
					t.Errorf("expected column %v to be %v, got %v", i, expect[i], c)
				}
			}
		}

		i := 0
		for rows.Next() {
			var name string
			var ts time.Time
			var wallet float64
			var bankrupt int
			var payday payday
			err := rows.Scan(&name, &ts, &wallet, &bankrupt, &payday)
			if err != nil {
				t.Errorf("scanning: %v", err)
			}
			if i == 0 {
				if name != "Ferengi" {
					t.Errorf("got incorrect name: %s", name)
				}
				if bankrupt != 0 {
					t.Errorf("got incorrect bankrupt: %d", bankrupt)
				}
			} else if i == 1 {
				if name != "Cardassian" {
					t.Errorf("got incorrect name: %s", name)
				}
				if bankrupt != 1 {
					t.Errorf("got incorrect bankrupt: %d", bankrupt)
				}
			}
			if ts.IsZero() || ts.Before(started) {
				t.Errorf("got incorrect time: %v", ts)
			}
			if wallet != 123.456 {
				t.Errorf("got incorrect wallet: %g", wallet)
			}
			if payday != struct{ Met string }{
				Met: fmt.Sprint(time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC).Unix()),
			} {
				t.Errorf("got incorrect payday: %v", payday)
			}

			i++
		}

		if i != 2 {
			t.Errorf("expected 2 rows, got %v", i)
		}
	})

	t.Run("Invalid Query", func(t *testing.T) {
		_, err := globalDB.Query("INVALID QUERY")
		if err == nil {
			t.Errorf("expected error for invalid query, got nil")
		}
	})
}
