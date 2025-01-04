package raft

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/Bl4ck-h00d/stashdb/core/store"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/Bl4ck-h00d/stashdb/types"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
)

type RaftFSM struct {
	store    types.Store
	eventCh  chan *protobuf.Event
	metadata map[string]*protobuf.Metadata
	mu       sync.RWMutex
}

func NewRaftFSM(storageEngine, dataDir string) *RaftFSM {
	store, err := store.NewStore(storageEngine, dataDir)
	if err != nil {
		slog.Error("failed to initialize the store")
	}

	return &RaftFSM{
		store:   store,
		eventCh: make(chan *protobuf.Event, 1024), metadata: make(map[string]*protobuf.Metadata, 0),
	}
}

func (f *RaftFSM) Apply(l *raft.Log) interface{} {
	var event protobuf.Event
	err := proto.Unmarshal(l.Data, &event)
	if err != nil {
		slog.Error("failed to unmarshal raft event", slog.Any("error", err))
		return err
	}

	switch event.Type {
	case protobuf.EventType_Join:
		var joinReq protobuf.JoinRequest
		err := json.Unmarshal(event.Message.Value, &joinReq)
		if err != nil {
			slog.Error("failed to unmarshal join request", slog.Any("error", err))
			return err
		}

		res := f.applySetMetadata(joinReq.Id, joinReq.Node.Metadata)

		if res == nil {
			f.eventCh <- &event
		}

		return res

	}
	return nil
}

func (f *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (f *RaftFSM) Restore(rc io.ReadCloser) error {
	// Restore state from a snapshot
	return nil
}

func (f *RaftFSM) applyCreateBucket(name string) error {
	err := f.store.CreateBucket(name)
	if err != nil {
		slog.Error("failed to create bucket", slog.String("bucket", name), slog.Any("error", err))
	}
	return nil
}

func (f *RaftFSM) Get(bucket, key string) ([]byte, error) {
	val := f.store.Get(bucket, key)
	if val == nil {
		slog.Error("key not found", slog.String("key", key), slog.String("bucket", bucket))
		return nil, errors.New("key not found")
	}
	return val, nil
}

func (f *RaftFSM) applySet(bucket, key string, value []byte) error {
	err := f.store.Set(bucket, key, value)
	if err != nil {
		slog.Error("failed to set key", slog.String("key", key), slog.String("bucket", bucket), slog.Any("error", err))
	}
	return nil
}

func (f *RaftFSM) applyDelete(bucket, key string) error {
	err := f.store.Delete(bucket, key)
	if err != nil {
		slog.Error("failed to delete key", slog.String("key", key), slog.String("bucket", bucket), slog.Any("error", err))
	}
	return nil
}

func (f *RaftFSM) GetAllBuckets() ([]string, error) {
	val, err := f.store.GetAllBuckets()
	if err != nil {
		slog.Error("failed fetch buckets", slog.Any("error", err))
		return nil, err
	}
	return val, nil
}

func (f *RaftFSM) GetAllKeys(bucket string, limit int64) (map[string][]byte, error) {
	val, err := f.store.GetAllKeys(bucket, limit)
	if err != nil {
		slog.Error("failed fetch keys", slog.String("bucket", bucket), slog.Any("error", err))
		return nil, err
	}
	return val, nil
}

func (f *RaftFSM) getMetadata(id string) *protobuf.Metadata {
	if metadata, ok := f.metadata[id]; ok {
		return metadata
	} else {
		slog.Warn("metadata not found", slog.String("id", id))
		return nil
	}
}

func (f *RaftFSM) setMetadata(id string, metadata *protobuf.Metadata) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.metadata[id] = metadata
}

func (f *RaftFSM) deleteMetadata(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.metadata, id)
}

func (f *RaftFSM) applySetMetadata(id string, metadata *protobuf.Metadata) interface{} {
	slog.Debug("set metadata", slog.String("id", id), slog.Any("metadata", metadata))
	f.setMetadata(id, metadata)
	return nil
}

func (f *RaftFSM) applyDeleteMetadata(id string) interface{} {
	slog.Debug("delete metadata", slog.String("id", id))
	f.deleteMetadata(id)
	return nil
}

func (f *RaftFSM) Close() error {
	f.eventCh <- nil
	slog.Debug("event channel closed")

	err := f.store.Close()
	if err != nil {
		slog.Error("failed to close the store", slog.Any("error", err))
	}
	slog.Info("store closed successfully")

	return nil
}

// ----------------

type FSMSnapshot struct {
	store *types.Store
}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// Save state to a snapshot
	return nil
}

func (s *FSMSnapshot) Release() {
	// Release resources
}
