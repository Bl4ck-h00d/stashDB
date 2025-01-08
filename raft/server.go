package raft

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Bl4ck-h00d/stashdb/marshaler"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
)

type RaftServer struct {
	Id          string // server identifier
	fsm         *RaftFSM
	raftAddress string
	bootstrap   bool // if true, only node in cluster
	raftDir     string

	Raft *raft.Raft // The consensus mechanism

	watchClusterStopCh chan struct{} //trigger stop the cluster
	watchClusterDoneCh chan struct{} //graceful shutdown

	EventCh chan *protobuf.Event
}

func NewRaftServer(id, raftAddress, storageEngine, raftDir string, bootstrap bool) (*RaftServer, error) {
	file := fmt.Sprintf("%s-stash.db", id)
	storeFSMPath := filepath.Join(raftDir, file)
	fsm := NewRaftFSM(storageEngine, storeFSMPath)

	return &RaftServer{
		Id:          id,
		fsm:         fsm,
		raftAddress: raftAddress,
		bootstrap:   bootstrap,
		raftDir:     raftDir,

		watchClusterStopCh: make(chan struct{}),
		watchClusterDoneCh: make(chan struct{}),

		EventCh: make(chan *protobuf.Event, 1024),
	}, nil
}

// Start opens the store. If bootstrapp is set true, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (r *RaftServer) Start() error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(r.Id)
	config.SnapshotThreshold = 1024
	config.LogOutput = ioutil.Discard
	config.HeartbeatTimeout = 1000 * time.Millisecond
	config.ElectionTimeout = 1000 * time.Millisecond

	addr, err := net.ResolveTCPAddr("tcp", r.raftAddress)
	if err != nil {
		slog.Error("failed to resolve TCP address", slog.String("raft_address", r.raftAddress), slog.Any("error", err))
		return err
	}

	transport, err := raft.NewTCPTransport(r.raftAddress, addr, 3, 10*time.Second, ioutil.Discard)
	if err != nil {
		slog.Error("failed to create TCP transport", slog.String("raft_address", r.raftAddress), slog.Any("error", err))
		return err
	}

	// Wipe storage if starting fresh
	raftDir := filepath.Join(r.raftDir, "raft")
	slog.Warn("Wiping persistent Raft storage", slog.String("path", raftDir))
	if err := os.RemoveAll(raftDir); err != nil {
		slog.Error("failed to wipe raft directory", slog.String("path", raftDir), slog.Any("error", err))
		return err
	}
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		slog.Error("failed to recreate raft directory", slog.String("path", raftDir), slog.Any("error", err))
		return err
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(r.raftDir, 2, ioutil.Discard)
	if err != nil {
		slog.Error("failed to create file snapshot store", slog.String("path", r.raftDir), slog.Any("error", err))
		return err
	}

	// Create Raft storage directory
	raftDir = filepath.Join(r.raftDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		slog.Error("failed to create raft directory", slog.String("path", raftDir), slog.Any("error", err))
		return err
	}

	// Stable store using BoltDB
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.db"))
	if err != nil {
		slog.Error("failed to create stable store", slog.String("path", raftDir), slog.Any("error", err))
		return err
	}

	// Log store using a separate BoltDB instance
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "logs.db"))
	if err != nil {
		slog.Error("failed to create log store", slog.String("path", raftDir), slog.Any("error", err))
		return err
	}

	// Create Raft instance
	r.Raft, err = raft.NewRaft(config, r.fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		slog.Error("failed to create raft", slog.Any("config", config), slog.Any("error", err))
		return err
	}

	// Bootstrap if needed
	if r.bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.Raft.BootstrapCluster(configuration)
	}

	go func() {
		r.startWatchCluster(500 * time.Millisecond)
	}()

	slog.Info("Raft server started", slog.String("raft_address", r.raftAddress))
	return nil
}

func (r *RaftServer) Stop() error {
	r.EventCh <- nil
	slog.Debug("closed event channel")

	r.stopWatchCluster()

	if err := r.fsm.Close(); err != nil {
		slog.Error("failed to close FSM", slog.Any("error", err))
	}
	slog.Debug("closed Raft FSM")

	if future := r.Raft.Shutdown(); future.Error() != nil {
		slog.Error("failed to shutdown Raft", slog.Any("error", future.Error()))
		return future.Error()
	}

	slog.Info("Raft server has stopped", slog.String("raft_address", r.raftAddress))

	return nil
}

func (r *RaftServer) startWatchCluster(checkInterval time.Duration) {
	slog.Info("initiating Raft watch cluster")

	defer func() {
		close(r.watchClusterDoneCh)
	}()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeout := 60 * time.Second
	if _, err := r.DetectLeader(timeout); err != nil {
		slog.Error("leader detection took to long", slog.Any("error", err))
	}

	for {
		select {
		case <-r.watchClusterStopCh:
			slog.Info("stoping Raft watch cluster")
			return
		case <-r.Raft.LeaderCh():
			slog.Info("elected as leader", slog.String("leaderAddr", string(r.Raft.Leader())))
		case event := <-r.fsm.eventCh:
			r.EventCh <- event
		}
	}
}

func (r *RaftServer) stopWatchCluster() {
	close(r.watchClusterStopCh)
	<-r.watchClusterDoneCh
	slog.Info("stopping Raft watch cluster")
}

// DetectLeader periodically checks for a leader in the Raft cluster until a timeout occurs.
//
// Parameters:
// - timeout: The maximum duration to wait for a leader to be elected.
func (r *RaftServer) DetectLeader(timeout time.Duration) (raft.ServerAddress, error) {
	// emits event after each tick, on the returned channel
	// periodically check for leader
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// emits single event after timeout
	// upper limit till which we will check for leader, periodically
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C: // leader detected
			leaderAddr := r.Raft.Leader()
			if leaderAddr != "" {
				slog.Debug("leader detected", slog.String("address", string(leaderAddr)))
				return leaderAddr, nil
			}

		case <-timer.C: // timeout occurred
			err := errors.New("timeout waiting for leader")
			slog.Error("failed to detect leader", slog.Any("error", err))
			return "", err
		}
	}
}

func (s *RaftServer) GetLeaderID(timeout time.Duration) (raft.ServerID, error) {
	leaderAddr, err := s.DetectLeader(timeout)
	if err != nil {
		slog.Error("failed to detect leader", slog.Any("error", err))
		return "", err
	}

	cf := s.Raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		slog.Error("failed to get Raft configuration", slog.Any("error", err))
		return "", err
	}

	for _, srv := range cf.Configuration().Servers {
		if srv.Address == leaderAddr {
			slog.Debug("leader detected", slog.String("id", string(srv.ID)))
			return srv.ID, nil
		}
	}

	err = errors.New("failed to detect leader ID")
	slog.Error("failed to detect leader ID")
	return "", err
}

// Nodes returns a map of all nodes in the Raft cluster, including their Raft addresses and metadata.
func (r *RaftServer) Nodes() (map[string]*protobuf.Node, error) {
	conf := r.Raft.GetConfiguration()
	if err := conf.Error(); err != nil {
		slog.Error("failed to get Raft configuration", slog.Any("error", err))
		return nil, err
	}

	nodes := make(map[string]*protobuf.Node)
	for _, server := range conf.Configuration().Servers {
		nodes[string(server.ID)] = &protobuf.Node{
			RaftAddress: string(server.Address),
			Metadata:    r.fsm.getMetadata(string(server.ID)),
		}
	}

	return nodes, nil
}

// Node returns information about the current Raft server node, including its Raft address, metadata, and state.
func (r *RaftServer) Node() (*protobuf.Node, error) {
	nodes, err := r.Nodes()
	if err != nil {
		return nil, err
	}

	node, ok := nodes[r.Id]
	if !ok {
		return nil, fmt.Errorf("node %s not found in the cluster", r.Id)
	}

	node.State = r.Raft.State().String()

	return node, nil
}

func (r *RaftServer) Join(id string, node *protobuf.Node) error {
	nodeExists, err := r.Exists(id)
	if err != nil {
		return err
	}

	if nodeExists {
		slog.Info("peer already exists in the cluster", slog.String("id", id), slog.String("raft-address", node.RaftAddress))
	} else {
		slog.Info("adding peer to the cluster", slog.String("id", id))
		future := r.Raft.AddVoter(raft.ServerID(id), raft.ServerAddress(node.RaftAddress), 0, 0)

		if future.Error() != nil {
			slog.Error("failed to add peer", slog.String("id", id), slog.String("raft-address", node.RaftAddress), slog.Any("error", future.Error()))
			return future.Error()
		}

		slog.Info("peer has been added successfully", slog.String("id", id), slog.String("raft-address", node.RaftAddress))
	}

	err = r.publishJoinEvent(id, node.Metadata)

	if err != nil {
		slog.Error("failed to set metadata", slog.String("id", id), slog.Any("metadata", node.Metadata), slog.Any("error", err))
		return err
	}

	slog.Info("node metadata updated successfully", slog.String("id", id), slog.Any("metadata", node.Metadata))
	return nil
}

func (r *RaftServer) publishJoinEvent(id string, metadata *protobuf.Metadata) error {

	data := &protobuf.SetMetadataRequest{
		Id:       id,
		Metadata: metadata,
	}

	dataAny := &any.Any{}
	err := marshaler.UnmarshalAny(data, dataAny)
	if err != nil {
		slog.Error("failed to unmarshal request to the command data", slog.String("id", id), slog.Any("metadata", metadata), slog.Any("error", err))
		return err
	}

	c := &protobuf.Event{
		Type:    protobuf.EventType_Join,
		Message: dataAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		slog.Error("failed to marshal the command into the bytes as message", slog.String("id", id), slog.Any("metadata", metadata), slog.Any("error", err))
		return err
	}

	// Apply the message
	f := r.Raft.Apply(msg, 10*time.Second)
	if err = f.Error(); err != nil {
		slog.Error("failed to apply message", slog.String("id", id), slog.Any("metadata", metadata), slog.Any("error", err))
		return err
	}

	return nil
}

func (r *RaftServer) Exists(id string) (bool, error) {
	exists := false

	cf := r.Raft.GetConfiguration()
	if cf.Error() != nil {
		slog.Error("failed to get Raft configuration", slog.String("id", id))
		return exists, cf.Error()
	}

	for _, srv := range cf.Configuration().Servers {
		if srv.ID == raft.ServerID(id) {
			slog.Info("node with this ID already exists in the cluster", slog.String("id", id))
			exists = true
			break
		}
	}

	return exists, nil

}

func (r *RaftServer) Leave(id string) error {
	nodeExists, err := r.Exists(id)
	if err != nil {
		return err
	}

	if nodeExists {
		if future := r.Raft.RemoveServer(raft.ServerID(id), 0, 0); future.Error() != nil {
			slog.Error("failed to remove peer", slog.String("id", id))
			return future.Error()
		}
		slog.Info("peer left the cluster", slog.String("id", id))
	} else {
		slog.Debug("peer does not exist", slog.String("id", id))
	}

	err = r.publishLeaveEvent(id)
	if err != nil {
		slog.Error("failed to leave the cluster", slog.String("id", id), slog.Any("error", err))
		return err
	}
	return nil
}

func (r *RaftServer) publishLeaveEvent(id string) error {
	data := &protobuf.DeleteMetadataRequest{
		Id: id,
	}

	dataAny := &any.Any{}
	err := marshaler.UnmarshalAny(data, dataAny)
	if err != nil {
		slog.Error("failed to unmarshal request to the command data", slog.String("id", id), slog.Any("error", err))
		return err
	}

	c := &protobuf.Event{
		Type:    protobuf.EventType_Leave,
		Message: dataAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		slog.Error("failed to marshal the command into the bytes as the message", slog.String("id", id), slog.Any("error", err))
		return err
	}
	if f := r.Raft.Apply(msg, 10*time.Second); f.Error() != nil {
		slog.Error("failed to apply message", slog.String("id", id), slog.Any("error", f.Error()))
		return f.Error()
	}

	return nil
}

// CRUD utilities

func (r *RaftServer) CreateBucket(req *protobuf.CreateBucketRequest) error {
	reqAny := &any.Any{}
	if err := marshaler.UnmarshalAny(req, reqAny); err != nil {
		slog.Error("failed to unmarshal request to the command data", slog.String("key", req.Name), slog.Any("error", err))
		return err
	}

	c := &protobuf.Event{
		Type:    protobuf.EventType_Create,
		Message: reqAny,
	}

	// Marshal the Event into bytes
	msg, err := proto.Marshal(c)
	if err != nil {
		slog.Error("failed to marshal the command into bytes as the message", slog.Any("request", req), slog.Any("error", err))
		return err
	}

	if f := r.Raft.Apply(msg, 10*time.Second); f.Error() != nil {
		slog.Error("failed to create bucket", slog.String("bucket", req.Name), slog.Any("error", f.Error()))
		return f.Error()
	}
	return nil
}

func (r *RaftServer) Get(req *protobuf.GetRequest) (*protobuf.GetResponse, error) {
	resp, err := r.fsm.Get(req.Bucket, req.Key)
	if resp == nil || resp.Value == nil {
		slog.Error("key not found", slog.String("bucket", req.Bucket), slog.String("key", req.Key))
		return nil, err
	}

	res := &protobuf.GetResponse{
		Value:     []byte(resp.Value),
		Timestamp: resp.Timestamp,
	}

	return res, nil
}

func (r *RaftServer) Set(req *protobuf.SetRequest) error {
	slog.Info("SET request - raft")

	reqAny := &any.Any{}
	if err := marshaler.UnmarshalAny(req, reqAny); err != nil {
		slog.Error("failed to unmarshal request to the command data", slog.String("key", req.Key), slog.Any("error", err))
		return err
	}

	c := &protobuf.Event{
		Type:    protobuf.EventType_Set,
		Message: reqAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		slog.Error("failed to marshal the command into the bytes as the message", slog.String("key", req.Key), slog.Any("error", err))
		return err
	}
	if f := r.Raft.Apply(msg, 10*time.Second); f.Error() != nil {
		slog.Error("failed to SET value", slog.String("bucket", req.Bucket), slog.String("key", req.Key), slog.Any("error", f.Error()))
		return f.Error()
	}
	return nil
}

func (r *RaftServer) Delete(req *protobuf.DeleteRequest) error {
	reqAny := &any.Any{}
	if err := marshaler.UnmarshalAny(req, reqAny); err != nil {
		slog.Error("failed to unmarshal request to the command data", slog.String("key", req.Key), slog.Any("error", err))
		return err
	}

	c := &protobuf.Event{
		Type:    protobuf.EventType_Delete,
		Message: reqAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		slog.Error("failed to marshal the command into the bytes as the message", slog.String("key", req.Key), slog.Any("error", err))
		return err
	}

	if f := r.Raft.Apply(msg, 10*time.Second); f.Error() != nil {
		slog.Error("failed to DELETE value", slog.String("bucket", req.Bucket), slog.String("key", req.Key), slog.Any("error", f.Error()))
		return f.Error()
	}
	return nil
}

func (r *RaftServer) GetAllBuckets() (*protobuf.GetAllBucketsResponse, error) {
	buckets, err := r.fsm.GetAllBuckets()
	if err != nil {
		slog.Error("failed to get all buckets", slog.Any("error", err))
		return nil, err
	}

	resp := &protobuf.GetAllBucketsResponse{
		Buckets: buckets,
	}

	return resp, nil
}

func (r *RaftServer) GetAllKeys(req *protobuf.GetAllKeysRequest) (*protobuf.GetAllKeysResponse, error) {
	keys, err := r.fsm.GetAllKeys(req.Bucket, req.Limit)
	if err != nil {
		slog.Error("failed to get all keys for bucket", slog.String("bucket", req.Bucket), slog.Any("error", err))
		return nil, err
	}

	resp := &protobuf.GetAllKeysResponse{
		Pairs: keys,
	}

	return resp, nil
}
