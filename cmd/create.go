package cmd

import (
	"context"
	"fmt"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	createCmd = &cobra.Command{
		Use:   "create BUCKET",
		Args:  cobra.ExactArgs(1),
		Short: "Create a new bucket",
		RunE: func(cmd *cobra.Command, args []string) error {
			grpcAddress := viper.GetString("grpc-address")
			certificateFile = viper.GetString("certificate-file")
			commonName = viper.GetString("common-name")

			bucket := args[0]

			c, err := client.NewGRPCClientWithContextTLS(context.Background(), grpcAddress, certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			req := &protobuf.CreateBucketRequest{
				Name: bucket,
			}

			err = c.CreateBucket(req)
			if err != nil {
				return err
			}

			fmt.Printf("Bucket created: %s\n", bucket)

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(createCmd)

	createCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", ":9000", "gRPC server listend address port")
	viper.BindPFlag("grpc-address", setCmd.PersistentFlags().Lookup("grpc-address"))
}
