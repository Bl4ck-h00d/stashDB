package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	joinCmd = &cobra.Command{
		Use:   "join <ID> <GRPC_ADDRESS>",
		Args:  cobra.ExactArgs(2),
		Short: "Join a node to the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			grpcAddress = viper.GetString("grpc-address")

			certificateFile = viper.GetString("certificate-file")
			commonName = viper.GetString("common-name")

			id := args[0]
			targetGrpcAddress := args[1]

			t, err := client.NewGRPCClientWithContextTLS(context.Background(), targetGrpcAddress, certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = t.Close()
			}()

			nodeResp, err := t.Node()
			if err != nil {
				return err
			}

			c, err := client.NewGRPCClientWithContextTLS(context.Background(), grpcAddress, certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			req := &protobuf.JoinRequest{
				Id:   id,
				Node: nodeResp.Node,
			}

			err = c.Join(req)
			if err != nil {
				return err
			}

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(joinCmd)

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

	joinCmd.PersistentFlags().StringVar(&configFile, "config-file", "", "config file path. If omitted, stashDB.yaml will be searched in /config")
	joinCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", ":9000", "gRPC server listen address")
	joinCmd.PersistentFlags().StringVar(&certificateFile, "certificate-file", "", "path to the client server TLS certificate file")
	joinCmd.PersistentFlags().StringVar(&commonName, "common-name", "", "certificate common name")

	_ = viper.BindPFlag("grpc-address", joinCmd.PersistentFlags().Lookup("grpc-address"))
	_ = viper.BindPFlag("certificate-file", joinCmd.PersistentFlags().Lookup("certificate-file"))
	_ = viper.BindPFlag("common-name", joinCmd.PersistentFlags().Lookup("common-name"))
}
