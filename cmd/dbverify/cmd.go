package main

import (
	"github.com/cjfinnell/dbverify"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "dbverify",
	RunE: func(cmd *cobra.Command, args []string) error {
		config := dbverify.Config{}
		return dbverify.Verify(config)
	},
}
