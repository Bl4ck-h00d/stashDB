package cmd

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Bl4ck-h00d/stashdb/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	startCmd = &cobra.Command{
		Use:   "start",
		Short: "start the stashDB server",
		Run: func(cmd *cobra.Command, args []string) {
			log.Println("Initialising stashDB...")
			id = viper.GetString("id")
			grpcPort = viper.GetString("grpc-port")
			httpPort = viper.GetString("http-port")
			dataPath = viper.GetString("data-path")
			storageEngine = viper.GetString("storage-engine")

			quitCh := make(chan os.Signal, 1)
			signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

			grpcServer, err := server.NewGRPCServer(grpcPort, storageEngine, dataPath)
			if err != nil {
				fmt.Printf("Failed to register gRPC server: %v", err)
				os.Exit(1)
			}

			if err := grpcServer.Start(); err != nil {
				fmt.Printf("Failed to start gRPC server: %v", err)
				os.Exit(1)
			}


			// wait for receiving signal
			<-quitCh

			_ = grpcServer.Stop()
		},
	}
)

func init() {
	rootCmd.AddCommand(startCmd)

	// Read config file
	cobra.OnInitialize(func() {
		if configFile != "" {
			viper.SetConfigFile(configFile)
		} else {
			viper.AddConfigPath("/config")
			viper.SetConfigName("stashDB")
		}

		viper.SetEnvPrefix("STASH_DB")
		viper.AutomaticEnv()

		if err := viper.ReadInConfig(); err != nil {
			switch err.(type) {
			case viper.ConfigFileNotFoundError:
				// stashDB.yaml not found in config search path
			default:
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	})

	// Flags
	startCmd.PersistentFlags().StringVar(&configFile, "config", "", "config file path. If omitted, stashDB.yaml will be searched in /config")
	startCmd.PersistentFlags().StringVar(&id, "id", "node1", "node ID")
	startCmd.PersistentFlags().StringVar(&grpcPort, "grpc-port", ":9000", "gRPC server listend address port")
	startCmd.PersistentFlags().StringVar(&httpPort, "http-port", ":8080", "HTTP server listend address port")
	startCmd.PersistentFlags().StringVar(&storageEngine, "storage-engine", "bolt", "set storage engine")
	startCmd.PersistentFlags().StringVar(&dataPath, "data-path", "data/stash.db", "data directory path which stores the key-value store data and logs")

	// Viper bindings
	viper.BindPFlag("id", startCmd.PersistentFlags().Lookup("id"))
	viper.BindPFlag("grpc-port", startCmd.PersistentFlags().Lookup("grpc-port"))
	viper.BindPFlag("http-port", startCmd.PersistentFlags().Lookup("http-port"))
	viper.BindPFlag("data-path", startCmd.PersistentFlags().Lookup("data-path"))
	viper.BindPFlag("storage-engine", startCmd.PersistentFlags().Lookup("storage-engine"))

}
