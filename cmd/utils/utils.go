package utils

import (
	"context"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/spf13/viper"
)

func GetGrpcClient() (*client.GRPCClient, error) {
	grpcAddress := viper.GetString("grpc-address")
	certificateFile := viper.GetString("certificate-file")
	commonName := viper.GetString("common-name")

	return client.NewGRPCClientWithContextTLS(context.Background(), grpcAddress, certificateFile, commonName)
}
