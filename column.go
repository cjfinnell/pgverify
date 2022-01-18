package dbverify

import (
	"fmt"
	"strings"
)

type column struct {
	name     string
	dataType string
}

func (c column) String() string {
	switch strings.ToLower(c.dataType) {
	case "timestamp with time zone":
		return fmt.Sprintf("extract(epoch from %s)::TEXT", c.name)
	default:
		return c.name + "::TEXT"
	}
}
