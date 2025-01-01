package main

import (
	"os"

	"github.com/Bl4ck-h00d/stashdb/cmd"
)

func main() {

	// server := api.NewAPIServer(config.Envs.Port, config.Envs.DbPath)

	// if err := server.Start(); err != nil {
	// 	log.Fatalf("error starting API server: %v", err)
	// }

	// CLI
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
