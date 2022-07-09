package gorqlite_test

import (
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/rqlite/gorqlite"
)

var globalConnection *gorqlite.Connection

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())

	conn, err := gorqlite.Open(testUrl())
	if err != nil {
		log.Fatalf("opening connection: %v", err)
	}

	_, err = conn.Write([]string{
		`CREATE TABLE IF NOT EXISTS ` + testTableName() + ` (id integer, name text)`,
		`CREATE TABLE IF NOT EXISTS ` + testTableName() + `_full (id integer, name text, wallet real, bankrupt boolean, payload blob, ts DATETIME)`,
	})
	if err != nil {
		log.Fatalf("creating table: %v", err)
	}

	globalConnection = conn

	exitCode := m.Run()

	_, err = conn.Write([]string{
		`DROP TABLE IF EXISTS ` + testTableName(),
		`DROP TABLE IF EXISTS ` + testTableName() + `_full`,
	})
	if err != nil {
		log.Fatalf("deleting table: %v", err)
	}

	conn.Close()

	os.Exit(exitCode)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func testUrl() string {
	url := os.Getenv("GORQLITE_TEST_URL")
	if url == "" {
		url = "http://"
	}
	return url
}

func testTableName() string {
	tableName := os.Getenv("GORQLITE_TEST_TABLE")
	if tableName == "" {
		tableName = "gorqlite_test"
	}
	return tableName
}
