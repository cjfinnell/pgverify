package pgverify

import (
	"fmt"
	"strings"
)

// column represents a column in a table.
type column struct {
	name        string
	dataType    string
	constraints []string
}

// IsPrimaryKey attempts to parse the constraint string to determine if the
// column is a primary key.
func (c column) IsPrimaryKey() bool {
	for _, constraint := range c.constraints {
		if constraint == "primary" || strings.HasSuffix(constraint, "_pkey") {
			return true
		}
	}

	return false
}

// CastToText generates PSQL expression to cast the column to the TEXT type in
// a way that is consistent between supported databases.
func (c column) CastToText() string {
	switch strings.ToLower(c.dataType) {
	case "timestamp with time zone":
		// Truncating the epoch means that timestamps will be compared "to the second"; timestamps with ms/ns differences will be considered equal.
		return fmt.Sprintf("trunc(extract(epoch from %s)::NUMERIC)::TEXT", c.name)
	default:
		return c.name + "::TEXT"
	}
}
