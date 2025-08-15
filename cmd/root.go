package cmd

import (
	"os"
	"fmt"
	"github.com/spf13/cobra"
	"legion/cluster"
)

var (
	verbose bool
	format string
)

// base of all cli commands, every command will start with "legion"
var rootCLI = &cobra.Command{
	Use: "legion",
	Short: "short description shown in help",
	Long: "long description",
	Version: "0.1.0",
}

// calls cobras built in Execute() function that starts CLI
func ExecuteCLI() {
	err := rootCLI.Execute() // start the cli and store error
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// flags, init() is automatically run by go
func init() {
	// persistent flags makes it so flags can be used for all rootCLI subcommands
	rootCLI.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	rootCLI.PersistentFlags().StringVarP(&format, "format", "f", "table", "output format (table, json, yaml)")
}