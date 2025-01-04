package client

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type GRPCClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	conn   *grpc.ClientConn
	client protobuf.StashDBServiceClient
}

func NewGRPCClient(grpcAddress string) (*GRPCClient, error) {
	return NewGRPCClientWithContext(context.Background(), grpcAddress)
}

func NewGRPCClientWithContext(ctx context.Context, grpcAddress string) (*GRPCClient, error) {
	return NewGRPCClientWithContextTLS(ctx, grpcAddress, "", "")
}

func NewGRPCClientWithContextTLS(ctx context.Context, grpcAddress string, certificateFile string, commonName string) (*GRPCClient, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(math.MaxInt64),
			grpc.MaxCallRecvMsgSize(math.MaxInt64),
		),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                1 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		),
	}

	ctx, cancel := context.WithCancel(ctx)

	if certificateFile == "" {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		creds, err := credentials.NewClientTLSFromFile(certificateFile, commonName)
		if err != nil {
			return nil, err
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	}

	conn, err := grpc.DialContext(ctx, grpcAddress, dialOpts...)
	if err != nil {
		cancel()
		return nil, err
	}

	return &GRPCClient{
		ctx:    ctx,
		cancel: cancel,
		conn:   conn,
		client: protobuf.NewStashDBServiceClient(conn),
	}, nil
}

// Target returns the target address of the gRPC connection.
//
// The target address is the address of the server that the client is currently
// connected to. If the client is not connected, this method will return an empty
// string.
//
// Return value:
// - string: The target address of the gRPC connection.
func (c *GRPCClient) Target() string {
    return c.conn.Target()
}

func (c *GRPCClient) Close() error {
	c.cancel()
	if c.conn != nil {
		return c.conn.Close()
	}

	return c.ctx.Err()
}

func (c *GRPCClient) CreateBucket(req *protobuf.CreateBucketRequest, opts ...grpc.CallOption) error {
	if _, err := c.client.CreateBucket(c.ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

func (c *GRPCClient) Get(req *protobuf.GetRequest, opts ...grpc.CallOption) (*protobuf.GetResponse, error) {
	if resp, err := c.client.Get(c.ctx, req, opts...); err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.NotFound:
			return nil, errors.New("Not found")
		default:
			return nil, err
		}
	} else {
		return resp, nil
	}
}

func (c *GRPCClient) Set(req *protobuf.SetRequest, opts ...grpc.CallOption) error {
	if _, err := c.client.Set(c.ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

func (c *GRPCClient) Delete(req *protobuf.DeleteRequest, opts ...grpc.CallOption) error {
	if _, err := c.client.Delete(c.ctx, req, opts...); err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.NotFound:
			return errors.New("Not found")
		default:
			return err
		}
	}

	return nil
}

func (c *GRPCClient) GetAllBuckets(req *empty.Empty, opts ...grpc.CallOption) (*protobuf.GetAllBucketsResponse, error) {
	if resp, err := c.client.GetAllBuckets(c.ctx, req, opts...); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

func (c *GRPCClient) GetAllKeys(req *protobuf.GetAllKeysRequest, opts ...grpc.CallOption) (*protobuf.GetAllKeysResponse, error) {
	if resp, err := c.client.GetAllKeys(c.ctx, req, opts...); err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.NotFound:
			return nil, errors.New("Not found")
		default:
			return nil, err
		}
	} else {
		return resp, nil
	}
}
