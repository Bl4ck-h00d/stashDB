package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/Bl4ck-h00d/stashdb/raft"
	"github.com/Bl4ck-h00d/stashdb/types"
	"github.com/golang/protobuf/ptypes/empty"
)

type GRPCService struct {
	protobuf.UnimplementedStashDBServiceServer
	db         types.Store
	raftServer *raft.RaftServer

	certificateFile string
	commonName      string

	// allows the server to broadcast events to all watchers
	watchChans map[chan *protobuf.WatchResponse]struct{} // set of watch channels

	peerGrpcClients map[string]*client.GRPCClient // map of peer grpc clients

	watchClusterStopCh chan struct{}
	watchClusterDoneCh chan struct{}

	watchMutex sync.RWMutex
}

func NewGRPCService(db types.Store, certificateFile, commonName string, raftServer *raft.RaftServer) *GRPCService {
	return &GRPCService{
		db:              db,
		raftServer:      raftServer,
		watchChans:      make(map[chan *protobuf.WatchResponse]struct{}),
		peerGrpcClients: make(map[string]*client.GRPCClient, 0),

		certificateFile: certificateFile,
		commonName:      commonName,

		watchClusterStopCh: make(chan struct{}),
		watchClusterDoneCh: make(chan struct{}),
	}
}

func (s *GRPCService) Start() error {
	go func() {
		s.startWatchCluster(500 * time.Millisecond)
	}()

	slog.Info("gRPC service started")
	return nil
}

func (s *GRPCService) Stop() error {
	s.stopWatchCluster()

	slog.Info("gRPC service stopped")
	return nil
}

// startWatchCluster initiates a watch cluster for the gRPC service.
// It listens for events from the Raft server and periodically checks for updates in peer gRPC clients.
//
// The function takes a time interval as a parameter, which determines the frequency of checking for updates in peer gRPC clients.
// It creates a ticker with the specified interval and starts a loop to listen for events and perform the necessary actions.
//
// The function also handles leader detection and logs any errors encountered during the process.
func (s *GRPCService) startWatchCluster(interval time.Duration) {
	slog.Info("Initiating gRPC watch cluster")

	defer func() {
		close(s.watchClusterDoneCh)
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	timeout := 60 * time.Second // leader detection timeout
	if err := s.raftServer.DetectLeader(timeout); err != nil {
		slog.Error("leader detection took to long", slog.Any("error", err))
	}

	for {
		select {
		case <-s.watchClusterStopCh:
			slog.Info("stopping grpc watch cluster")
			return
		case event := <-s.raftServer.EventCh:
			watchResp := &protobuf.WatchResponse{
				Event: event,
			}

			// notify clients
			for c := range s.watchChans {
				c <- watchResp
			}

		case <-ticker.C:
			s.updatePeerGrpcClients()
		}
	}
}

func (s *GRPCService) updatePeerGrpcClients() {
	s.watchMutex.Lock()
	defer s.watchMutex.Unlock()

	nodes, err := s.raftServer.Nodes()
	if err != nil {
		slog.Error("failed to get nodes from raft cluster", slog.Any("error", err))
		return
	}

	// create new clients for peer nodes if not present
	for id, node := range nodes {
		if id == s.raftServer.Id {
			continue
		}

		if node.Metadata == nil || node.Metadata.GrpcAddress == "" {
			slog.Debug("gRPC address is missing", slog.String("id", id))
		}

		if c, ok := s.peerGrpcClients[id]; ok {

			// NOTE: If the peer node is restarted or reconfigured, its gRPC address might change. In such cases, the client might still be holding on to the old address.

			// If the current client's target address differs from the node's gRPC address
			if c.Target() != node.Metadata.GrpcAddress {
				// close the client
				slog.Debug("close client", slog.String("id", id), slog.String("grpc-address", c.Target()))

				delete(s.peerGrpcClients, id)

				if err := c.Close(); err != nil {
					slog.Warn("failed to close client", slog.String("id", id), slog.String("grpc-address", c.Target()))
				}

				// create new client
				slog.Debug("create client", slog.String("id", id), slog.String("grpc-address", c.Target()))

				newClient, err := client.NewGRPCClientWithContextTLS(context.Background(), node.Metadata.GrpcAddress, s.certificateFile, s.commonName)

				if err != nil {
					slog.Error("failed to create client", slog.String("id", id), slog.String("grpc-address", node.Metadata.GrpcAddress))
					continue
				}

				s.peerGrpcClients[id] = newClient

			}
		} else {
			// node not in the peerList, create a new client

			slog.Debug("create client", slog.String("id", id), slog.String("grpc-address", c.Target()))

			newClient, err := client.NewGRPCClientWithContextTLS(context.Background(), node.Metadata.GrpcAddress, s.certificateFile, s.commonName)

			if err != nil {
				slog.Error("failed to create client", slog.String("id", id), slog.String("grpc-address", node.Metadata.GrpcAddress))
				continue
			}

			s.peerGrpcClients[id] = newClient
		}
	}

	// cleanup client
	for id, c := range s.peerGrpcClients {
		if _, exists := nodes[id]; !exists {
			// close client
			slog.Debug("close client", slog.String("id", id), slog.String("grpc-address", c.Target()))

			delete(s.peerGrpcClients, id)

			if err := c.Close(); err != nil {
				slog.Warn("failed to close client", slog.String("id", id), slog.String("grpc-address", c.Target()))
			}
		}
	}
}

func (s *GRPCService) stopWatchCluster() {
	slog.Info("initiate watch cluster shutdown")
	close(s.watchClusterStopCh)

	slog.Info("gracefully shutting down watch cluster")
	<-s.watchClusterDoneCh
	slog.Info("watch cluster shutdown completed")

	//client cleanup
	slog.Info("shutting down peer clients")
	for id, c := range s.peerGrpcClients {
		slog.Debug("close client", slog.String("id", id), slog.String("grpc-address", c.Target()))

		delete(s.peerGrpcClients, id)

		if err := c.Close(); err != nil {
			slog.Warn("failed to close client", slog.String("id", id), slog.String("grpc-address", c.Target()))
		}
	}
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
