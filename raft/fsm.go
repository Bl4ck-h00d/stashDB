package raft

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/Bl4ck-h00d/stashdb/core/store"
	"github.com/Bl4ck-h00d/stashdb/marshaler"
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

func NewRaftFSM(storageEngine, dataDir string) (*RaftFSM, error) {
	store, err := store.NewStore(storageEngine, dataDir)
	if err != nil {
		slog.Error("failed to initialize the store", slog.Any("error", err))
		return nil, err
	}

	return &RaftFSM{
		store:   store,
		eventCh: make(chan *protobuf.Event, 1024),
		metadata: make(map[string]*protobuf.Metadata),
	}, nil
}

func (f *RaftFSM) Apply(l *raft.Log) interface{} {
	var event protobuf.Event
	if err := proto.Unmarshal(l.Data, &event); err != nil {
		slog.Error("failed to unmarshal log data", slog.Any("error", err))
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.metadata[""] = &protobuf.Metadata{}

	switch event.Type {
	case protobuf.EventType_Join:
		data, err := marshaler.MarshalAny(event.Message)
		if err != nil {
			slog.Error("failed to marshal join event", slog.Any("error", err))
			return err
		}
		req := data.(*protobuf.SetMetadataRequest)
		return f.applySetMetadata(req.Id, req.Metadata)

	case protobuf.EventType_Set:
		data, err := marshaler.MarshalAny(event.Message)
		if err != nil {
			slog.Error("failed to marshal set event", slog.Any("error", err))
			return err
		}
		req := data.(*protobuf.SetRequest)
		return f.applySet(req.Bucket, req.Key, []byte(req.Value))

	case protobuf.EventType_Delete:
		data, err := marshaler.MarshalAny(event.Message)
		if err != nil {
			slog.Error("failed to marshal delete event", slog.Any("error", err))
			return err
		}
		req := data.(*protobuf.DeleteRequest)
		if req.Key == "" {
			slog.Error("key is empty, delete failed")
			return errors.New("key is empty")
		}
		return f.applyDelete(req.Bucket, req.Key)

	default:
		slog.Warn("unknown event type", slog.Int("type", int(event.Type)))
		return errors.New("unknown event type")
	}
}

func (f *RaftFSM) applySet(bucket, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	slog.Debug("SET Request", slog.String("bucket", bucket), slog.String("key", key), slog.String("value", string(value)))
	if err := f.store.Set(bucket, key, value); err != nil {
		slog.Error("failed to set value", slog.Any("error", err))
		return err
	}
	return nil
}

func (f *RaftFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		slog.Error("failed to read restore data", slog.Any("error", err))
		return err
	}

	buff := proto.NewBuffer(data)
	for {
		kvp := &protobuf.KeyValuePair{}
		if err := buff.DecodeMessage(kvp); err != nil {
			if err == io.EOF {
				break
			}
			slog.Warn("failed to decode key-value pair", slog.Any("error", err))
			continue
		}
		if kvp.Key == "" {
			slog.Warn("skipping corrupt data with empty key")
			continue
		}
		if err := f.store.Set("", kvp.Key, kvp.Value); err != nil {
			slog.Error("failed to restore key-value pair", slog.Any("error", err))
			return err
		}
	}

	return nil
}

func (f *RaftFSM) Get(bucket, key string) (*protobuf.GetResponse, error) {
	slog.Debug("GET Request", slog.String("bucket", bucket), slog.String("key", key))
	resp, err := f.store.Get(bucket, key)
	if err != nil || resp == nil || resp.Value == nil {
		slog.Error("key not found", slog.String("key", key), slog.String("bucket", bucket))
		return nil, errors.New("key not found")
	}
	return &protobuf.GetResponse{Value: resp.Value, Timestamp: resp.Timestamp}, nil
}

func (f *RaftFSM) Close() error {
	close(f.eventCh)
	if err := f.store.Close(); err != nil {
		slog.Error("failed to close store", slog.Any("error", err))
		return err
	}
	slog.Info("store closed successfully")
	return nil
}

// ----------------

type FSMSnapshot struct {
	store types.Store
}

func (f *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{store: f.store}, nil
}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic during snapshot persist", slog.Any("reason", r))
			sink.Cancel()
		} else {
			if err := sink.Close(); err != nil {
				slog.Error("failed to close sink", slog.Any("error", err))
			}
		}
	}()

	ch := s.store.SnapshotItems()
	kvpCount := uint64(0)

	for kvp := range ch {
		if kvp == nil {
			break
		}

		buff := proto.NewBuffer(nil)
		if err := buff.EncodeMessage(kvp); err != nil {
			slog.Error("failed to encode kvp", slog.String("key", kvp.Key), slog.Any("error", err))
			sink.Cancel()
			return err
		}

		if _, err := sink.Write(buff.Bytes()); err != nil {
			slog.Error("failed to write kvp", slog.String("key", kvp.Key), slog.Any("error", err))
			sink.Cancel()
			return err
		}
		kvpCount++
	}

	slog.Info("snapshot persisted", slog.Uint64("count", kvpCount), slog.Float64("duration", time.Since(start).Seconds()))
	return nil
}

func (s *FSMSnapshot) Release() {
	slog.Info("snapshot released")
}
