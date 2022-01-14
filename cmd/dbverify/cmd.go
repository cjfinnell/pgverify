package main

import (
	"fmt"

	"github.com/cjfinnell/dbverify"
	"github.com/jackc/pgx"
	"github.com/spf13/cobra"
)

// Flags
var (
	targetsFlag []string
)

func init() {
	rootCmd.Flags().StringSliceP("targets", "t", targetsFlag, "target database URIs (comma separated)")
	rootCmd.MarkFlagRequired("targets") //nolint:errcheck
}

var rootCmd = &cobra.Command{
	Use: "dbverify",
	RunE: func(cmd *cobra.Command, args []string) error {
		var targets []pgx.ConnConfig
		for _, target := range targetsFlag {
			connConfig, err := pgx.ParseURI(target)
			if err != nil {
				return fmt.Errorf("invalid target URI %s: %w", target, err)
			}
			targets = append(targets, connConfig)
		}

		return dbverify.Verify(targets)
	},
}
