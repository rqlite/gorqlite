package gorqlite

import (
	"testing"
)

func Test_ParseConsistencyLevel(t *testing.T) {
	tests := []struct {
		name    string
		level   string
		want    consistencyLevel
		wantErr bool
	}{
		{
			name:  "none",
			level: "none",
			want:  ConsistencyLevelNone,
		},
		{
			name:  "weak",
			level: "weak",
			want:  ConsistencyLevelWeak,
		},
		{
			name:  "linearizable",
			level: "linearizable",
			want:  ConsistencyLevelLinearizable,
		},
		{
			name:  "strong",
			level: "strong",
			want:  ConsistencyLevelStrong,
		},
		{
			name:    "invalid",
			level:   "invalid",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseConsistencyLevel(test.level)
			if test.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if got != test.want {
				t.Errorf("got %v, want %v", got, test.want)
			}
		})
	}
}
