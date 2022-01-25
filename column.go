package pgverify

import (
	"fmt"
	"strings"
)

type column struct {
	name       string
	dataType   string
	constraint string
}

func (c column) IsPrimaryKey() bool {
	if c.constraint == "primary" || strings.HasSuffix(c.constraint, "_pkey") {
		return true
	}
	return false
}

func (c column) CastToText() string {
	switch strings.ToLower(c.dataType) {
	case "timestamp with time zone":
		return fmt.Sprintf("extract(epoch from %s)::TEXT", c.name)
	default:
		return c.name + "::TEXT"
	}
}
