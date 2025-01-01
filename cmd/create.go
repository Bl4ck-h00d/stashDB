package cmd

import (
	"fmt"

	"github.com/Bl4ck-h00d/stashdb/cmd/utils"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/spf13/cobra"
)

var (
	createCmd = &cobra.Command{
		Use:   "create BUCKET",
		Args:  cobra.ExactArgs(1),
		Short: "Create a new bucket",
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := args[0]

			c, err := utils.GetGrpcClient()
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
}
