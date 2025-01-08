package cmd

import (
	"context"
	"fmt"

	"github.com/Bl4ck-h00d/stashdb/benchmark"
	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	benchCmd = &cobra.Command{
		Use:   "benchmark",
		Short: "Benchmark",
		RunE: func(cmd *cobra.Command, args []string) error {
			certificateFile = viper.GetString("certificate-file")
			commonName = viper.GetString("common-name")

			c, err := client.NewGRPCClientWithContextTLS(context.Background(), grpcAddress, certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			fmt.Println("Benchmarking")
			benchmark.RunStressTest(c)

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(benchCmd)

	benchCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", ":9000", "gRPC server listend address port")
	viper.BindPFlag("grpc-address", benchCmd.PersistentFlags().Lookup("grpc-address"))

}
