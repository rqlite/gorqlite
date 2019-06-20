package gorqlite

import "os"

/*
	No actual tests here, just utilities used by testing
*/

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
