package cmd

import "github.com/spf13/cobra"

var (
	rootCmd = &cobra.Command{
		Use:   "stashdb",
		Short: "A lightweight distributed key-value store with support for different databases",
		Long:  "A lightweight distributed key-value store with support for different databases",
	}
)

func Execute() error {
	return rootCmd.Execute()
}
