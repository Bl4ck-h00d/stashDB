package cmd

import (
	"fmt"

	"github.com/Bl4ck-h00d/stashdb/cmd/utils"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/spf13/cobra"
)

var (
	deleteCmd = &cobra.Command{
		Use:   "delete BUCKET KEY",
		Args:  cobra.ExactArgs(2),
		Short: "Delete a key from a bucket",
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := args[0]
			key := args[1]

			c, err := utils.GetGrpcClient()
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			req := &protobuf.DeleteRequest{
				Key:    key,
				Bucket: bucket,
			}

			err = c.Delete(req)
			if err != nil {
				return err
			}

			fmt.Printf("Deleted key:%s in bucket:%s \n", key, bucket)

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(deleteCmd)
}
