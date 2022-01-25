package main

import (
	"fmt"

	"github.com/cjfinnell/pgverify"
	"github.com/jackc/pgx"
	"github.com/spf13/cobra"
)

// Flags
var (
	aliasesFlag, excludeSchemasFlag, excludeTablesFlag, includeSchemasFlag, includeTablesFlag *[]string
	strategyFlag                                                                              *string
)

func init() {
	aliasesFlag = rootCmd.Flags().StringSlice("aliases", []string{}, "alias names for the supplied targets (comma separated)")
	excludeSchemasFlag = rootCmd.Flags().StringSlice("exclude-schemas", []string{}, "schemas to skip verification, ignored if '--include-schemas' used (comma separated)")
	excludeTablesFlag = rootCmd.Flags().StringSlice("exclude-tables", []string{}, "tables to skip verification, ignored if '--include-tables' used (comma separated)")
	includeSchemasFlag = rootCmd.Flags().StringSlice("include-schemas", []string{}, "schemas to verify (comma separated)")
	includeTablesFlag = rootCmd.Flags().StringSlice("include-tables", []string{}, "tables to verify (comma separated)")

	strategyFlag = rootCmd.Flags().StringP("strategy", "s", pgverify.StrategyFull, "strategy to use for verification")
}

var rootCmd = &cobra.Command{
	Use:  "pgverify [flags] target-uri...",
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var targets []pgx.ConnConfig
		for _, target := range args {
			connConfig, err := pgx.ParseURI(target)
			if err != nil {
				return fmt.Errorf("invalid target URI %s: %w", target, err)
			}
			targets = append(targets, connConfig)
		}

		opts := []pgverify.Option{
			pgverify.IncludeTables(*includeTablesFlag...),
			pgverify.ExcludeTables(*excludeTablesFlag...),
			pgverify.IncludeSchemas(*includeSchemasFlag...),
			pgverify.ExcludeSchemas(*excludeSchemasFlag...),
			pgverify.WithStrategy(*strategyFlag),
		}

		if len(*aliasesFlag) > 0 {
			opts = append(opts, pgverify.WithAliases(*aliasesFlag))
		}

		report, err := pgverify.Verify(targets, opts...)
		if err != nil {
			return err
		}
		report.WriteAsTable(cmd.OutOrStdout())
		return nil
	},
}
