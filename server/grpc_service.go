package server

import (
	"context"

	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/Bl4ck-h00d/stashdb/types"
	"github.com/golang/protobuf/ptypes/empty"
)

type GRPCService struct {
	protobuf.UnimplementedStashDBServiceServer
	db types.Store
}

func NewGRPCService(db types.Store) *GRPCService {
	return &GRPCService{db: db}
}

func (s *GRPCService) CreateBucket(ctx context.Context, req *protobuf.CreateBucketRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	err := s.db.CreateBucket(req.Name)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *GRPCService) Get(ctx context.Context, req *protobuf.GetRequest) (*protobuf.GetResponse, error) {

	resp := &protobuf.GetResponse{}

	val := s.db.Get(req.Bucket, req.Key)
	resp.Value = val

	return resp, nil
}

func (s *GRPCService) Set(ctx context.Context, req *protobuf.SetRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}
	err := s.db.Set(req.Bucket, req.Key, []byte(req.Value))
	if err != nil {
		return resp, nil
	}

	return resp, nil
}

func (s *GRPCService) Delete(ctx context.Context, req *protobuf.DeleteRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}
	err := s.db.Delete(req.Bucket, req.Key)
	if err != nil {
		return resp, nil
	}

	return resp, nil
}

func (s *GRPCService) GetAllBuckets(ctx context.Context, req *empty.Empty) (*protobuf.GetAllBucketsResponse, error) {
	resp, err := s.db.GetAllBuckets()
	if err != nil {
		return nil, err
	}

	return &protobuf.GetAllBucketsResponse{Buckets: resp}, nil
}

func (s *GRPCService) GetAllKeys(ctx context.Context, req *protobuf.GetAllKeysRequest) (*protobuf.GetAllKeysResponse, error) {
	bucket := req.Bucket
	limit := req.Limit
	resp, err := s.db.GetAllKeys(bucket, limit)
	if err != nil {
		return nil, err
	}

	return &protobuf.GetAllKeysResponse{Pairs: resp}, nil
}
