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
	getCmd = &cobra.Command{
		Use:   "get BUCKET KEY",
		Args:  cobra.ExactArgs(2),
		Short: "Get a key from a bucket",
		RunE: func(cmd *cobra.Command, args []string) error {
			grpcPort := viper.GetString("grpc-port")
			certificateFile = viper.GetString("certificate-file")
			commonName = viper.GetString("common-name")

			bucket := args[0]
			key := args[1]

			c, err := client.NewGRPCClientWithContextTLS(context.Background(), grpcPort, certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			req := &protobuf.GetRequest{
				Key:    key,
				Bucket: bucket,
			}

			resp, err := c.Get(req)
			if err != nil {
				return err
			}

			fmt.Printf("%s/%s => %+v", bucket, key, string(resp.Value[:]))
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(getCmd)
}
