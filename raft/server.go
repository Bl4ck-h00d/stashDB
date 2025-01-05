package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
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
	storeFSMPath := filepath.Join(raftDir, "stashdb")
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

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(r.Id)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", r.raftAddress)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(r.raftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(r.raftDir, 2, os.Stderr)
	if err != nil {
		slog.Error("error creating snapshot store", slog.Any("error", err))
		return err
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(r.raftDir, "raft.db"),
	})
	if err != nil {
		slog.Error("failed to create log store and stable store", slog.Any("error", err))
		return err
	}
	logStore = boltDB
	stableStore = boltDB

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, r.fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		slog.Error("failed to instantiate raft system", slog.Any("error", err))
		return err
	}
	r.Raft = ra

	if r.bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
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
	slog.Info("Initiating Raft watch cluster")

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
			slog.Info("stoping watch cluster")
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
	slog.Info("cluster watcher has been stopped")
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
			slog.Info("leader detected", slog.String("id", string(srv.ID)))
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
		slog.Debug("node already exists in the cluster", slog.String("id", id), slog.String("raft-address", node.RaftAddress))
	} else {
		future := r.Raft.AddVoter(raft.ServerID(id), raft.ServerAddress(node.RaftAddress), 0, 0)

		if future.Error() != nil {
			slog.Error("failed to add peer", slog.String("id", id), slog.String("raft-address", node.RaftAddress), slog.Any("error", future.Error()))
			return future.Error()
		}

		slog.Info("node has been added successfully", slog.String("id", id), slog.String("raft-address", node.RaftAddress))
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

	// convert request to any type
	dataAny := &any.Any{}
	marshalledVal, _ := json.Marshal(data)
	json.Unmarshal(marshalledVal, dataAny)

	event := &protobuf.Event{
		Type:    protobuf.EventType_Join,
		Message: dataAny,
	}

	marshalledEvent, _ := json.Marshal(event)

	f := r.Raft.Apply(marshalledEvent, 10*time.Second)
	if err := f.Error(); err != nil {
		slog.Error("failed to apply message", slog.String("id", id), slog.Any("metadata", metadata), slog.Any("error", err))
		return err
	}

	return nil
}

func (r *RaftServer) Exists(id string) (bool, error) {
	return false, nil
}
