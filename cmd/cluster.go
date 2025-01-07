package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	clusterCmd = &cobra.Command{
		Use:   "cluster",
		Short: "Get the cluster info",
		RunE: func(cmd *cobra.Command, args []string) error {
			grpcAddress = viper.GetString("grpc-address")

			certificateFile = viper.GetString("certificate-file")
			commonName = viper.GetString("common-name")


			c, err := client.NewGRPCClientWithContextTLS(context.Background(), grpcAddress, certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			resp, err := c.Cluster()
			if err != nil {
				return err
			}

			respBytes, err := json.Marshal(resp)
			if err != nil {
				return err
			}

			fmt.Println(string(respBytes))

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(clusterCmd)

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

	clusterCmd.PersistentFlags().StringVar(&configFile, "config-file", "", "config file path. If omitted, stashDB.yaml will be searched in /config")
	clusterCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", ":9000", "gRPC server listen address")
	clusterCmd.PersistentFlags().StringVar(&certificateFile, "certificate-file", "", "path to the client server TLS certificate file")
	clusterCmd.PersistentFlags().StringVar(&commonName, "common-name", "", "certificate common name")

	_ = viper.BindPFlag("grpc-address", clusterCmd.PersistentFlags().Lookup("grpc-address"))
	_ = viper.BindPFlag("certificate-file", clusterCmd.PersistentFlags().Lookup("certificate-file"))
	_ = viper.BindPFlag("common-name", clusterCmd.PersistentFlags().Lookup("common-name"))
}
