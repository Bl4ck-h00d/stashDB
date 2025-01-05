package cmd

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/Bl4ck-h00d/stashdb/raft"
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
			grpcAddress = viper.GetString("grpc-address")
			httpAddress = viper.GetString("http-address")
			raftAddress = viper.GetString("raft-address")
			peerGrpcAddress = viper.GetString("peer-grpc-address") // gRPC address of any node in the cluster
			dataDir = viper.GetString("data-directory")
			storageEngine = viper.GetString("storage-engine")

			certificateFile = viper.GetString("certificate-file")
			commonName = viper.GetString("common-name")

			level := slog.LevelInfo // default

			switch strings.ToLower(logLevel) {
			case "debug":
				level = slog.LevelDebug
			case "info":
				level = slog.LevelInfo
			case "warn":
				level = slog.LevelWarn
			case "error":
				level = slog.LevelError
			}

			handlerOpts := &slog.HandlerOptions{
				Level: level,
			}
			logger := slog.New(slog.NewTextHandler(os.Stderr, handlerOpts))
			slog.SetDefault(logger)

			quitCh := make(chan os.Signal, 1)
			signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

			bootstrap := peerGrpcAddress == "" || peerGrpcAddress == grpcAddress

			raftServer, err := raft.NewRaftServer(id, raftAddress, storageEngine, dataDir, bootstrap)
			if err != nil {
				slog.Error("failed to create Raft server", slog.Any("error", err))
				os.Exit(1)
			}

			grpcServer, err := server.NewGRPCServer(grpcAddress, storageEngine, dataDir, certificateFile, commonName, raftServer)
			if err != nil {
				slog.Error("failed to create gRPC server", slog.Any("error", err))
				os.Exit(1)
			}

			if err := raftServer.Start(); err != nil {
				slog.Error("failed to start Raft server", slog.Any("error", err))
				os.Exit(1)
			}

			if err := grpcServer.Start(); err != nil {
				slog.Error("failed to start gRPC server", slog.Any("error", err))
				os.Exit(1)
			}

			if bootstrap {
				timeout := 60 * time.Second
				_, err := raftServer.DetectLeader(timeout)
				if err != nil {
					slog.Error("leader detection took to long", slog.Any("error", err))
					os.Exit(1)
				}
			}

			var joinGrpcAddress string
			if bootstrap {
				joinGrpcAddress = grpcAddress
			} else {
				joinGrpcAddress = peerGrpcAddress
			}

			// create a client connection to a peer node
			c, err := client.NewGRPCClientWithContextTLS(context.Background(), joinGrpcAddress, certificateFile, commonName)
			if err != nil {
				slog.Error("failed to create client", slog.Any("error", err))
				os.Exit(1)
			}

			defer func() {
				c.Close()
			}()

			//prepare join request
			joinReq := &protobuf.JoinRequest{
				Id: id,
				Node: &protobuf.Node{
					RaftAddress: raftAddress,
					Metadata: &protobuf.Metadata{
						GrpcAddress: grpcAddress,
						HttpAddress: httpAddress,
					},
				},
			}

			if err := c.Join(joinReq); err != nil {
				slog.Error("failed to join cluster", slog.Any("error", err))
			}

			// wait for receiving signal
			<-quitCh

			_ = grpcServer.Stop()
			_ = raftServer.Stop()
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
	startCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", ":9000", "gRPC server listend address port")
	startCmd.PersistentFlags().StringVar(&httpAddress, "http-address", ":8080", "HTTP server listend address port")
	startCmd.PersistentFlags().StringVar(&raftAddress, "raft-address", ":7000", "Cluster communication port")
	startCmd.PersistentFlags().StringVar(&storageEngine, "storage-engine", "bolt", "set storage engine")
	startCmd.PersistentFlags().StringVar(&dataDir, "data-dir", "data", "data directory path which stores the key-value store data and logs")
	startCmd.PersistentFlags().StringVar(&logLevel, "log", "info", "set debug level")

	startCmd.PersistentFlags().StringVar(&certificateFile, "certificate-file", "", "path to the client server TLS certificate file")
	startCmd.PersistentFlags().StringVar(&commonName, "common-name", "", "certificate common name")

	// Viper bindings
	viper.BindPFlag("id", startCmd.PersistentFlags().Lookup("id"))
	viper.BindPFlag("grpc-address", startCmd.PersistentFlags().Lookup("grpc-address"))
	viper.BindPFlag("http-address", startCmd.PersistentFlags().Lookup("http-address"))
	viper.BindPFlag("raft-address", startCmd.PersistentFlags().Lookup("raft-address"))
	viper.BindPFlag("data-directory", startCmd.PersistentFlags().Lookup("data-directory"))
	viper.BindPFlag("storage-engine", startCmd.PersistentFlags().Lookup("storage-engine"))
	viper.BindPFlag("log-level", startCmd.PersistentFlags().Lookup("log"))
	viper.BindPFlag("certificate-file", startCmd.PersistentFlags().Lookup("certificate-file"))
	viper.BindPFlag("common-name", startCmd.PersistentFlags().Lookup("common-name"))

}
