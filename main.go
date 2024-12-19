package main

import (
	"log"

	"github.com/Bl4ck-h00d/stashdb/api"
	"github.com/Bl4ck-h00d/stashdb/config"
)

func main() {

	server := api.NewAPIServer(config.Envs.Port, config.Envs.DbPath)

	if err := server.Start(); err != nil {
		log.Fatalf("error starting API server: %v", err)
	}
}
