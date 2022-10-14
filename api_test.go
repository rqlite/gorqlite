package gorqlite

import (
	"testing"
)

func Test_RedactURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "no credentials",
			url:  "http://localhost:4001/db/query",
			want: "http://localhost:4001/db/query",
		},
		{
			name: "with credentials",
			url:  "http://user:pass@localhost:4001/db/query",
			want: "http://user:xxxxx@localhost:4001/db/query",
		},
		{
			name: "with credentials and params",
			url:  "http://user:notasecret@localhost:4001/db/query?pretty=true",
			want: "http://user:xxxxx@localhost:4001/db/query?pretty=true",
		},
		{
			name: "invalid url",
			url:  "h{tp://invalid-url",
			want: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := redactURL(test.url)
			if got != test.want {
				t.Errorf("got %s, want %s", got, test.want)
			}
		})
	}
}
