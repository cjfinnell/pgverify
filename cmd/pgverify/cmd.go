package main

import (
	"strings"

	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cjfinnell/pgverify"
)

// Flags.
var (
	aliasesFlag, excludeSchemasFlag, excludeTablesFlag, includeSchemasFlag, includeTablesFlag, includeColumnsFlag, excludeColumnsFlag, testModesFlag *[]string
	logLevelFlag, timestampPrecisionFlag                                                                                                             *string
	bookendLimitFlag, sparseModFlag                                                                                                                  *int
	hashPrimaryKeysFlag                                                                                                                              *bool
)

func init() {
	aliasesFlag = rootCmd.Flags().StringSlice("aliases", []string{}, "alias names for the supplied targets (comma separated)")
	excludeSchemasFlag = rootCmd.Flags().StringSlice("exclude-schemas", []string{}, "schemas to skip verification, ignored if '--include-schemas' used (comma separated)")
	excludeTablesFlag = rootCmd.Flags().StringSlice("exclude-tables", []string{}, "tables to skip verification, ignored if '--include-tables' used (comma separated)")
	excludeColumnsFlag = rootCmd.Flags().StringSlice("exclude-columns", []string{}, "column names to skip verification, ignored if '--include-columns' used (comma separated)")
	includeSchemasFlag = rootCmd.Flags().StringSlice("include-schemas", []string{}, "schemas to verify (comma separated, defaults to all)")
	includeTablesFlag = rootCmd.Flags().StringSlice("include-tables", []string{}, "tables to verify (comma separated, defaults to all)")
	includeColumnsFlag = rootCmd.Flags().StringSlice("include-columns", []string{}, "columns to explicitly verify (comma separated, defaults to all)")

	timestampPrecisionFlag = rootCmd.Flags().String("tz-precision", "milliseconds", "precision level to use when comparing timestamps")
	logLevelFlag = rootCmd.Flags().String("level", "info", "logging level")
	testModesFlag = rootCmd.Flags().StringSliceP("tests", "t", []string{pgverify.TestModeFull},
		"tests to use for verification (comma separated, options: "+strings.Join([]string{
			pgverify.TestModeFull,
			pgverify.TestModeBookend,
			pgverify.TestModeSparse,
			pgverify.TestModeRowCount,
		}, ",")+")")

	bookendLimitFlag = rootCmd.Flags().Int("bookend-limit", pgverify.TestModeBookendDefaultLimit, "only check the first and last N rows (with --tests=bookend)")
	sparseModFlag = rootCmd.Flags().Int("sparse-mod", pgverify.TestModeSparseDefaultMod, "only check every Nth row (with --tests=sparse)")

	hashPrimaryKeysFlag = rootCmd.Flags().Bool("hash-primary-keys", false, "hash primary key values before comparing them (useful for TEXT primary keys)")
}

var rootCmd = &cobra.Command{
	Use:  "pgverify [flags] target-uri...",
	Long: `Verify data consistency between PostgreSQL syntax compatible databases.`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := log.New()
		logger.SetFormatter(&log.TextFormatter{})
		levelInt, err := log.ParseLevel(*logLevelFlag)
		if err != nil {
			levelInt = log.InfoLevel
		}
		logger.SetLevel(levelInt)

		if len(args) == 0 {
			logger.Fatal("requires at least 1 arg(s), received 0; see 'pgverify --help' for more information")
		}

		var targets []*pgx.ConnConfig
		for _, target := range args {
			connConfig, err := pgx.ParseConfig(target)
			if err != nil {
				logger.Fatalf("invalid target URI %s: %s", target, err)
			}
			targets = append(targets, connConfig)
		}

		opts := []pgverify.Option{
			pgverify.WithLogger(logger),
			pgverify.IncludeTables(*includeTablesFlag...),
			pgverify.ExcludeTables(*excludeTablesFlag...),
			pgverify.IncludeSchemas(*includeSchemasFlag...),
			pgverify.ExcludeSchemas(*excludeSchemasFlag...),
			pgverify.IncludeColumns(*includeColumnsFlag...),
			pgverify.ExcludeColumns(*excludeColumnsFlag...),
			pgverify.WithTests(*testModesFlag...),
			pgverify.WithSparseMod(*sparseModFlag),
			pgverify.WithBookendLimit(*bookendLimitFlag),
			pgverify.WithTimestampPrecision(*timestampPrecisionFlag),
		}

		if *hashPrimaryKeysFlag {
			opts = append(opts, pgverify.WithHashPrimaryKeys())
		}

		if len(*aliasesFlag) > 0 {
			opts = append(opts, pgverify.WithAliases(*aliasesFlag))
		}

		report, err := pgverify.Verify(cmd.Context(), targets, opts...)
		report.WriteAsTable(cmd.OutOrStdout())

		if err != nil {
			logger.Fatal(err)
		}
	},
}
