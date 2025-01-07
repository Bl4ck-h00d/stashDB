package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	watchCmd = &cobra.Command{
		Use:   "watch",
		Short: "Watch a node updates",
		Long:  "Watch a node updates",
		RunE: func(cmd *cobra.Command, args []string) error {
			grpcAddress = viper.GetString("grpc-address")

			certificateFile = viper.GetString("certificate_file")
			commonName = viper.GetString("common-name")

			c, err := client.NewGRPCClientWithContextTLS(context.Background(), grpcAddress, certificateFile, commonName)
			if err != nil {
				return err
			}
			defer func() {
				_ = c.Close()
			}()

			req := &empty.Empty{}
			watchClient, err := c.Watch(req)
			if err != nil {
				return err
			}

			go func() {
				for {
					resp, err := watchClient.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						break
					}

					switch resp.Event.Type {
					case protobuf.EventType_Join:
						// Unmarshal *anypb.Any into the expected protobuf message type
						eventReq := &protobuf.SetMetadataRequest{}
						if err := resp.Event.Message.UnmarshalTo(eventReq); err != nil {
							_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, failed to unmarshal: %v", resp.Event.Type.String(), err))
						} else {
							// Successfully unmarshalled, print the event request
							fmt.Printf("%s, %v\n", resp.Event.Type.String(), protojson.Format(eventReq))
						}
					case protobuf.EventType_Leave:
						eventReq := &protobuf.DeleteMetadataRequest{}
						if err := resp.Event.Message.UnmarshalTo(eventReq); err != nil {
							_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, failed to unmarshal: %v", resp.Event.Type.String(), err))
						} else {
							fmt.Printf("%s, %v\n", resp.Event.Type.String(), protojson.Format(eventReq))
						}
					case protobuf.EventType_Set:
						putRequest := &protobuf.SetRequest{}
						if err := resp.Event.Message.UnmarshalTo(putRequest); err != nil {
							_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, failed to unmarshal: %v", resp.Event.Type.String(), err))
						} else {
							fmt.Printf("%s, %v\n", resp.Event.Type.String(), protojson.Format(putRequest))
						}
					case protobuf.EventType_Delete:
						deleteRequest := &protobuf.DeleteRequest{}
						if err := resp.Event.Message.UnmarshalTo(deleteRequest); err != nil {
							_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, failed to unmarshal: %v", resp.Event.Type.String(), err))
						} else {
							fmt.Printf("%s, %v\n", resp.Event.Type.String(), protojson.Format(deleteRequest))
						}
					default:
						fmt.Printf("Unhandled event type: %s\n", resp.Event.Type.String())
					}
				}
			}()

			quitCh := make(chan os.Signal, 1)
			signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

			<-quitCh

			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(watchCmd)

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

	watchCmd.PersistentFlags().StringVar(&configFile, "config-file", "", "config file. if omitted, stashDB.yaml in /econfig and home directory will be searched")
	watchCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", ":9000", "gRPC server listen address")
	watchCmd.PersistentFlags().StringVar(&certificateFile, "certificate-file", "", "path to the client server TLS certificate file")
	watchCmd.PersistentFlags().StringVar(&commonName, "common-name", "", "certificate common name")

	_ = viper.BindPFlag("grpc-address", watchCmd.PersistentFlags().Lookup("grpc-address"))
	_ = viper.BindPFlag("certificate-file", watchCmd.PersistentFlags().Lookup("certificate-file"))
	_ = viper.BindPFlag("common-name", watchCmd.PersistentFlags().Lookup("common-name"))
}
