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
	leaveCmd = &cobra.Command{
		Use:   "leave <ID>",
		Args:  cobra.ExactArgs(1),
		Short: "Remove a node from the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			grpcAddress = viper.GetString("grpc-address")

			certificateFile = viper.GetString("certificate-file")
			commonName = viper.GetString("common-name")

			id := args[0]

			c, err := client.NewGRPCClientWithContextTLS(context.Background(), grpcAddress, certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()


			req := &protobuf.LeaveRequest{
				Id: id,
			}

			if err := c.Leave(req); err != nil {
				return err
			}

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(leaveCmd)

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

	leaveCmd.PersistentFlags().StringVar(&configFile, "config-file", "", "config file path. If omitted, stashDB.yaml will be searched in /config")
	leaveCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", ":9000", "gRPC server listen address")
	leaveCmd.PersistentFlags().StringVar(&certificateFile, "certificate-file", "", "path to the client server TLS certificate file")
	leaveCmd.PersistentFlags().StringVar(&commonName, "common-name", "", "certificate common name")

	_ = viper.BindPFlag("grpc-address", leaveCmd.PersistentFlags().Lookup("grpc-address"))
	_ = viper.BindPFlag("certificate-file", leaveCmd.PersistentFlags().Lookup("certificate-file"))
	_ = viper.BindPFlag("common-name", leaveCmd.PersistentFlags().Lookup("common-name"))
}
