package gorqlite

import "fmt"

type consistencyLevel int

const (
	// ConsistencyLevelNone provides no consistency to other nodes.
	ConsistencyLevelNone consistencyLevel = iota
	// ConsistencyLevelWeak provides a weak consistency that guarantees the
	// queries are sent to the leader.
	ConsistencyLevelWeak
	// ConsitencyLevelLinearizable provides a linearizable consistency and
	// guarantees that read result will reflect all previous writes.
	ConsistencyLevelLinearizable
	// ConsistencyLevelStrong provides a strong consistency and guarantees
	// that the read result will reflect all previous writes and that all
	// previously commmitted writes in the Raft log have been applied..
	ConsistencyLevelStrong
)

var consistencyLevelToString = map[consistencyLevel]string{
	ConsistencyLevelNone:         "none",
	ConsistencyLevelWeak:         "weak",
	ConsistencyLevelLinearizable: "linearizable",
	ConsistencyLevelStrong:       "strong",
}

// String returns the string representation of a consistencyLevel.
func (c consistencyLevel) String() string {
	return consistencyLevelToString[c]
}

// ParseConsistencyLevel parses a string into a consistencyLevel.
func ParseConsistencyLevel(s string) (consistencyLevel, error) {
	for k, v := range consistencyLevelToString {
		if v == s {
			return k, nil
		}
	}
	return ConsistencyLevelNone, fmt.Errorf("unknown consistency level: %s", s)
}
