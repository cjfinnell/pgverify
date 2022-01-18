package main

import (
	"fmt"

	"github.com/cjfinnell/pgverify"
	"github.com/jackc/pgx"
	"github.com/spf13/cobra"
)

// Flags
var (
	targetsFlag        *[]string
	includeTablesFlag  *[]string
	excludeTablesFlag  *[]string
	includeSchemasFlag *[]string
	excludeSchemasFlag *[]string
)

func init() {
	targetsFlag = rootCmd.Flags().StringSliceP("targets", "t", []string{}, "target postgresql-format database URIs (comma separated)")
	rootCmd.MarkFlagRequired("targets") //nolint:errcheck

	includeTablesFlag = rootCmd.Flags().StringSlice("include-tables", []string{}, "tables to verify (comma separated)")
	excludeTablesFlag = rootCmd.Flags().StringSlice("exclude-tables", []string{}, "tables to skip verification, ignored if '--include-tables' used (comma separated)")
	includeSchemasFlag = rootCmd.Flags().StringSlice("include-schemas", []string{}, "schemas to verify (comma separated)")
	excludeSchemasFlag = rootCmd.Flags().StringSlice("exclude-schemas", []string{}, "schemas to skip verification, ignored if '--include-schemas' used (comma separated)")
}

var rootCmd = &cobra.Command{
	Use: "pgverify",
	RunE: func(cmd *cobra.Command, args []string) error {
		var targets []pgx.ConnConfig
		for _, target := range *targetsFlag {
			connConfig, err := pgx.ParseURI(target)
			if err != nil {
				return fmt.Errorf("invalid target URI %s: %w", target, err)
			}
			targets = append(targets, connConfig)
		}

		report, err := pgverify.Verify(
			targets,
			pgverify.IncludeTables(*includeTablesFlag...),
			pgverify.ExcludeTables(*excludeTablesFlag...),
			pgverify.IncludeSchemas(*includeSchemasFlag...),
			pgverify.ExcludeSchemas(*excludeSchemasFlag...),
		)
		if err != nil {
			return err
		}
		report.WriteAsTable(cmd.OutOrStdout())
		return nil
	},
}
