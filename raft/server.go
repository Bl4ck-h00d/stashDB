package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type RaftServer struct {
	Id          string // server identifier
	fsm         *RaftFSM
	raftAddress string
	bootstrap   bool // if true, only node in cluster
	raftDir     string

	raft *raft.Raft // The consensus mechanism

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
	r.raft = ra

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

	if future := r.raft.Shutdown(); future.Error() != nil {
		slog.Error("failed to shutdown Raft", slog.Any("error", future.Error()))
		return future.Error()
	}

	slog.Info("Raft server has stopped", slog.String("raft_address", r.raftAddress))

	return nil
}

func (r *RaftServer) startWatchCluster(checkInterval time.Duration) {
	slog.Info("Initiating Raft watch cluster")

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeout := 60 * time.Second
	if err := r.DetectLeader(timeout); err != nil {
		slog.Error("leader detection took to long", slog.Any("error", err))
	}

	for {
		select {
		case <-r.watchClusterStopCh:
			slog.Info("stoping watch cluster")
			return
		case <-r.raft.LeaderCh():
			slog.Info("elected as leader", slog.String("leaderAddr", string(r.raft.Leader())))
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
//
// Returns:
// - nil: If a leader is detected within the specified timeout.
// - error: If no leader is detected within the specified timeout.
func (r *RaftServer) DetectLeader(timeout time.Duration) error {
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
			leaderAddr := r.raft.Leader()
			if leaderAddr != "" {
				slog.Debug("leader detected", slog.String("address", string(leaderAddr)))
				return nil
			}

		case <-timer.C: // timeout occurred
			err := errors.New("timeout waiting for leader")
			slog.Error("failed to detect leader", slog.Any("error", err))
			return err
		}
	}
}

// Nodes returns a map of all nodes in the Raft cluster, including their Raft addresses and metadata.
func (r *RaftServer) Nodes() (map[string]*protobuf.Node, error) {
	conf := r.raft.GetConfiguration()
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

func (r *RaftServer) Node() (*protobuf.Node, error) {
	nodes, err := r.Nodes()
	if err != nil {
		return nil, err
	}

	node, ok := nodes[r.Id]
	if !ok {
		return nil, fmt.Errorf("node %s not found in the cluster", r.Id)
	}

	node.State = r.raft.State().String()

	return node, nil
}
