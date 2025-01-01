package utils

import (
	"context"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/spf13/viper"
)

func GetGrpcClient() (*client.GRPCClient, error) {
	grpcPort := viper.GetString("grpc-port")
	certificateFile := viper.GetString("certificate-file")
	commonName := viper.GetString("common-name")

	return client.NewGRPCClientWithContextTLS(context.Background(), grpcPort, certificateFile, commonName)
}
