package cmd

import (
	"fmt"

	"github.com/Bl4ck-h00d/stashdb/cmd/utils"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/spf13/cobra"
)

var (
	detailBucket string
	limit        int

	listCmd = &cobra.Command{
		Use:   "ls",
		Short: "Lists buckets or key-value pairs within a bucket",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Establish gRPC connection
			c, err := utils.GetGrpcClient()
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			if detailBucket == "" {
				// List all buckets
				resp, err := c.GetAllBuckets(nil)
				if err != nil {
					return err
				}

				fmt.Println("Buckets:")
				for _, bucket := range resp.Buckets {
					fmt.Println(" -", bucket)
				}
			} else {
				// List key-value pairs in a specific bucket
				resp, err := c.GetAllKeys(&protobuf.GetAllKeysRequest{
					Bucket: detailBucket,
					Limit:  int64(limit),
				})
				if err != nil {
					return err
				}

				fmt.Printf("Key-Value pairs in bucket '%s':\n", detailBucket)
				for k, v := range resp.Pairs {
					fmt.Printf(" - Key: %s, Value: %s\n", k, v)
				}
			}

			return nil
		},
	}
)

func init() {
	listCmd.Flags().StringVarP(&detailBucket, "bucket", "d", "", "Specify the bucket to list key-value pairs from")
	listCmd.Flags().IntVar(&limit, "limit", 10, "Limit the number of key-value pairs listed")
	rootCmd.AddCommand(listCmd)
}
