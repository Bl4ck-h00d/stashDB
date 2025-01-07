package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/Bl4ck-h00d/stashdb/raft"
	"github.com/golang/protobuf/ptypes/empty"
	_raft "github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCService struct {
	protobuf.UnimplementedStashDBServiceServer
	raftServer *raft.RaftServer

	certificateFile string
	commonName      string

	// allows the server to broadcast events to all watchers
	watchChans map[chan protobuf.WatchResponse]struct{} // set of watch channels

	peerGrpcClients map[string]*client.GRPCClient // map of peer grpc clients

	watchClusterStopCh chan struct{}
	watchClusterDoneCh chan struct{}

	watchMutex sync.RWMutex
}

func NewGRPCService(certificateFile, commonName string, raftServer *raft.RaftServer) *GRPCService {
	return &GRPCService{
		raftServer:      raftServer,
		watchChans:      make(map[chan protobuf.WatchResponse]struct{}),
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

func (s *GRPCService) Watch(req *empty.Empty, server protobuf.StashDBService_WatchServer) error {
	chans := make(chan protobuf.WatchResponse)

	s.watchMutex.Lock()
	s.watchChans[chans] = struct{}{}
	s.watchMutex.Unlock()

	defer func() {
		s.watchMutex.Lock()
		delete(s.watchChans, chans)
		s.watchMutex.Unlock()
		close(chans)
	}()

	for resp := range chans {
		if err := server.Send(&resp); err != nil {
			slog.Error("failed to send watch data", slog.String("event", resp.Event.String()), slog.Any("error", err))
			return status.Error(codes.Internal, err.Error())
		}
	}

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
	slog.Info("initiating gRPC watch cluster")

	defer func() {
		close(s.watchClusterDoneCh)
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	timeout := 60 * time.Second // leader detection timeout
	if _, err := s.raftServer.DetectLeader(timeout); err != nil {
		slog.Error("leader detection took to long", slog.Any("error", err))
	}

	for {
		select {
		case <-s.watchClusterStopCh:
			slog.Info("stopping gRPC watch cluster")
			return
		case event := <-s.raftServer.EventCh:
			watchResp := &protobuf.WatchResponse{
				Event: event,
			}

			// notify clients
			for c := range s.watchChans {
				c <- *watchResp
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
			continue
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

			slog.Debug("creating new client", slog.String("id", id))

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

	err := s.raftServer.CreateBucket(req)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *GRPCService) Get(ctx context.Context, req *protobuf.GetRequest) (*protobuf.GetResponse, error) {

	resp, err := s.raftServer.Get(req)

	return resp, err
}

func (s *GRPCService) Set(ctx context.Context, req *protobuf.SetRequest) (*empty.Empty, error) {
	slog.Info("SET request")
	resp := &empty.Empty{}

	if s.raftServer.Raft.State() != _raft.Leader {
		clusterResp, err := s.Cluster(ctx, &empty.Empty{})
		if err != nil {
			slog.Error("failed to get cluster info", slog.Any("error", err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c := s.peerGrpcClients[clusterResp.Cluster.Leader]
		err = c.Set(req)
		if err != nil {
			slog.Error("failed to forward request", slog.String("grpc-address", c.Target()), slog.Any("error", err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Set(req)
	if err != nil {
		slog.Error("failed to put data", slog.Any("req", req), slog.Any("error", err))
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *GRPCService) Delete(ctx context.Context, req *protobuf.DeleteRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.Raft.State() != _raft.Leader {
		clusterResp, err := s.Cluster(ctx, &empty.Empty{})
		if err != nil {
			slog.Error("failed to get cluster info", slog.Any("error", err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c := s.peerGrpcClients[clusterResp.Cluster.Leader]
		err = c.Delete(req)
		if err != nil {
			slog.Error("failed to forward request", slog.String("grpc-address", c.Target()), slog.Any("error", err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Delete(req)
	if err != nil {
		slog.Error("failed to put data", slog.Any("req", req), slog.Any("error", err))
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *GRPCService) GetAllBuckets(ctx context.Context, req *empty.Empty) (*protobuf.GetAllBucketsResponse, error) {
	resp, err := s.raftServer.GetAllBuckets()
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *GRPCService) GetAllKeys(ctx context.Context, req *protobuf.GetAllKeysRequest) (*protobuf.GetAllKeysResponse, error) {
	resp, err := s.raftServer.GetAllKeys(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *GRPCService) Join(ctx context.Context, req *protobuf.JoinRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}
	// check if the current node is a leader
	// if not then forward the join request to the leader
	if s.raftServer.Raft.State() != _raft.Leader {
		// get the leader node

		cluster, err := s.Cluster(ctx, &empty.Empty{})
		if err != nil {
			slog.Error("failed to get cluster info", slog.Any("error", err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		// get grpc client for leader node
		c := s.peerGrpcClients[cluster.Cluster.Leader]

		// forward join request to the leader node
		err = c.Join(req)
		if err != nil {
			slog.Error("failed to forward request to leader node", slog.Any("error", err))
			return resp, err
		}

		return resp, nil
	}
	slog.Debug("leader received join request", slog.String("ID", req.Id))
	// if current node is the leader
	err := s.raftServer.Join(req.Id, req.Node)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *GRPCService) Node(ctx context.Context, req *empty.Empty) (*protobuf.NodeResponse, error) {
	resp := &protobuf.NodeResponse{}

	node, err := s.raftServer.Node()
	if err != nil {
		slog.Error("failed to get node info", slog.Any("err", err.Error()))
		return resp, status.Error(codes.Internal, err.Error())
	}

	resp.Node = node

	return resp, nil
}

func (s *GRPCService) Cluster(ctx context.Context, req *empty.Empty) (*protobuf.ClusterResponse, error) {
	resp := &protobuf.ClusterResponse{}

	cluster := &protobuf.Cluster{}

	// get map of nodes in the cluster
	nodes, err := s.raftServer.Nodes()
	if err != nil {
		slog.Error("failed to get cluster info", slog.Any("error", err))
		return resp, err
	}

	// update the node states
	for id, node := range nodes {
		if id == s.raftServer.Id {
			node.State = s.raftServer.Raft.State().String()
		} else {
			c := s.peerGrpcClients[id]
			nodeResp, err := c.Node()
			if err != nil {
				slog.Error("failed to get node info", slog.String("id", id), slog.Any("error", err))
			} else {
				node.State = nodeResp.Node.State
			}
		}
	}

	slog.Debug("updating cluster")

	// update the cluster
	cluster.Nodes = nodes

	// get the leader
	serverId, err := s.raftServer.GetLeaderID(60 * time.Second)
	if err != nil {
		slog.Error("failed to get cluster info", slog.Any("error", err))
		return resp, err
	}

	cluster.Leader = string(serverId)
	resp.Cluster = cluster
	slog.Debug("cluster updated", slog.Any("cluster", cluster))

	return resp, nil
}

func (s *GRPCService) Leave(ctx context.Context, req *protobuf.LeaveRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.Raft.State() != _raft.Leader {
		clusterResp, err := s.Cluster(ctx, &empty.Empty{})
		if err != nil {
			slog.Error("failed to get cluster info", slog.Any("error", err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c := s.peerGrpcClients[clusterResp.Cluster.Leader]
		err = c.Leave(req)
		if err != nil {
			slog.Error("failed to forward request", slog.String("grpc_address", c.Target()), slog.Any("error", err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Leave(req.Id)
	if err != nil {
		slog.Error("failed to leave node from the cluster", slog.Any("req", req), slog.Any("error", err))
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
