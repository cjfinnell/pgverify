package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var expectedHelpOutput = `
Verify data consistency between PostgreSQL syntax compatible databases.

Usage:
  pgverify [flags] target-uri...

Flags:
      --aliases strings           alias names for the supplied targets (comma separated)
      --bookend-limit int         only check the first and last N rows (with --tests=bookend) (default 1000)
      --exclude-columns strings   column names to skip verification, ignored if '--include-columns' used (comma separated)
      --exclude-schemas strings   schemas to skip verification, ignored if '--include-schemas' used (comma separated)
      --exclude-tables strings    tables to skip verification, ignored if '--include-tables' used (comma separated)
      --hash-primary-keys         hash primary key values before comparing them (useful for TEXT primary keys)
      --include-columns strings   columns to explicitly verify (comma separated, defaults to all)
      --include-schemas strings   schemas to verify (comma separated, defaults to all)
      --include-tables strings    tables to verify (comma separated, defaults to all)
      --level string              logging level (default "info")
      --sparse-mod int            only check every Nth row (with --tests=sparse) (default 10)
  -t, --tests strings             tests to use for verification (comma separated, options: full,bookend,sparse,rowcount) (default [full])
      --tz-precision string       precision level to use when comparing timestamps (default "milliseconds")`

func TestCmdHelp(t *testing.T) {
	bufOut := new(bytes.Buffer)
	bufErr := new(bytes.Buffer)
	testCmd := rootCmd
	testCmd.SetOut(bufOut)
	testCmd.SetErr(bufErr)

	require.NoError(t, testCmd.Help())
	require.Empty(t, bufErr.String())
	require.Equal(t,
		strings.TrimSpace(expectedHelpOutput),
		strings.TrimSpace(strings.ReplaceAll(bufOut.String(), "\t", "  ")),
	)
}
