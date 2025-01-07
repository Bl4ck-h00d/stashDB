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
	setCmd = &cobra.Command{
		Use:   "set BUCKET KEY VALUE",
		Args:  cobra.ExactArgs(3),
		Short: "Set a key-value",
		RunE: func(cmd *cobra.Command, args []string) error {
			grpcAddress := viper.GetString("grpc-address")
			certificateFile = viper.GetString("certificate-file")
			commonName = viper.GetString("common-name")

			bucket := args[0]
			key := args[1]
			value := args[2]

			c, err := client.NewGRPCClientWithContextTLS(context.Background(), grpcAddress, certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			req := &protobuf.SetRequest{
				Key:    key,
				Value:  value,
				Bucket: bucket,
			}

			if err := c.Set(req); err != nil {
				return err
			}
			fmt.Println("query executed")


			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(setCmd)

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
	setCmd.PersistentFlags().StringVar(&configFile, "config", "", "config file path. If omitted, stashDB.yaml will be searched in /config")
	setCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", ":9000", "gRPC server listend address port")
	setCmd.PersistentFlags().StringVar(&certificateFile, "certificate-file", "", "path to the client server TLS certificate file")
	setCmd.PersistentFlags().StringVar(&commonName, "common-name", "", "certificate common name")

	// Viper bindings
	viper.BindPFlag("grpc-address", setCmd.PersistentFlags().Lookup("grpc-address"))
	viper.BindPFlag("certificate-file", setCmd.PersistentFlags().Lookup("certificate-file"))
	viper.BindPFlag("common-name", setCmd.PersistentFlags().Lookup("common-name"))
}
