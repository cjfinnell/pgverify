package pgverify

import (
	"fmt"
	"strings"
)

// column represents a column in a table
type column struct {
	name       string
	dataType   string
	constraint string
}

// IsPrimaryKey attempts to parse the constraint string to determine if the
// column is a primary key.
func (c column) IsPrimaryKey() bool {
	if c.constraint == "primary" || strings.HasSuffix(c.constraint, "_pkey") {
		return true
	}
	return false
}

// CastToText generates PSQL expression to cast the column to the TEXT type in
// a way that is consistent between supported databases.
func (c column) CastToText() string {
	switch strings.ToLower(c.dataType) {
	case "timestamp with time zone":
		return fmt.Sprintf("extract(epoch from %s)::TEXT", c.name)
	default:
		return c.name + "::TEXT"
	}
}
