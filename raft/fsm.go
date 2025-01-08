package raft

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log/slog"
	"sync"
	"time"

	"github.com/Bl4ck-h00d/stashdb/core/store"
	"github.com/Bl4ck-h00d/stashdb/marshaler"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/Bl4ck-h00d/stashdb/types"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/encoding/protojson"
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
		// If binary unmarshaling fails, try JSON unmarshaling
		fmt.Printf("Binary Unmarshal failed, trying JSON: %v\n", err)
		err = protojson.Unmarshal(l.Data, &event)
		if err != nil {
			fmt.Printf("JSON Unmarshal also failed: %v\n", err)
			return nil
		}
	}

	switch event.Type {
	case protobuf.EventType_Join:
		data, err := marshaler.MarshalAny(event.Message)

		if err != nil {
			slog.Error("failed to marshal join request", slog.Any("error", err))
			return err
		}
		if data == nil {
			err = errors.New("nil")
			slog.Error("request is nil", slog.String("type", event.Type.String()))
			return err
		}

		req := data.(*protobuf.SetMetadataRequest)

		res := f.applySetMetadata(req.Id, req.Metadata)

		if res == nil {
			f.eventCh <- &event
		}

		return res
	case protobuf.EventType_Leave:
		data, err := marshaler.MarshalAny(event.Message)
		if err != nil {
			slog.Error("failed to unmarshal leave request", slog.Any("error", err))
			return err
		}
		req := *data.(*protobuf.DeleteMetadataRequest)
		res := f.applyDeleteMetadata(req.Id)

		if res == nil {
			f.eventCh <- &event
		}

		return res

	case protobuf.EventType_Create:
		data, err := marshaler.MarshalAny(event.Message)
		if err != nil {
			slog.Error("failed to unmarshal create bucket request", slog.Any("error", err))
			return err
		}

		req := *data.(*protobuf.CreateBucketRequest)
		res := f.applyCreateBucket(req.Name)
		if res == nil {
			f.eventCh <- &event
		}

		return res
	case protobuf.EventType_Set:
		data, err := marshaler.MarshalAny(event.Message)
		if err != nil {
			slog.Error("failed to unmarshal set request", slog.Any("error", err))
			return err
		}
		req := *data.(*protobuf.SetRequest)
		res := f.applySet(req.Bucket, req.Key, []byte(req.Value))
		if res == nil {
			f.eventCh <- &event
		}

		return res
	case protobuf.EventType_Delete:
		data, err := marshaler.MarshalAny(event.Message)
		if err != nil {
			slog.Error("failed to unmarshal delete request", slog.Any("error", err))
			return err
		}

		req := *data.(*protobuf.DeleteRequest)
		res := f.applyDelete(req.Bucket, req.Key)
		if res == nil {
			f.eventCh <- &event
		}

		return res
	default:
		err = errors.New("command type not support")
		slog.Error("unsupported command", slog.String("type", event.Type.String()))
		return err
	}
	return nil
}

func (f *RaftFSM) applyCreateBucket(name string) error {
	err := f.store.CreateBucket(name)
	if err != nil {
		slog.Error("failed to create bucket", slog.String("bucket", name), slog.Any("error", err))
	}
	return nil
}

func (f *RaftFSM) Get(bucket, key string) (*protobuf.GetResponse, error) {
	slog.Debug("GET Request", slog.String("bucket", bucket), slog.String("key", key))
	resp, _ := f.store.Get(bucket, key)
	
	if resp == nil || resp.Value == nil {
		slog.Error("key not found", slog.String("key", key), slog.String("bucket", bucket))
		return nil, errors.New("key not found")
	}
	protoRes := &protobuf.GetResponse{
		Value:     resp.Value,
		Timestamp: resp.Timestamp,
	}
	return protoRes, nil
}

func (f *RaftFSM) applySet(bucket, key string, value []byte) error {
	slog.Debug("SET Request", slog.String("bucket", bucket), slog.String("key", key), slog.String("value", string(value)))
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

func (f *RaftFSM) GetAllKeys(bucket string, limit int64) (map[string]*protobuf.GetResponse, error) {

	protoResp := make(map[string]*protobuf.GetResponse, 0)
	resp, err := f.store.GetAllKeys(bucket, limit)
	for key, val := range resp {
		value := &protobuf.GetResponse{
			Value:     val.Value,
			Timestamp: val.Timestamp,
		}
		protoResp[key] = value
	}
	if err != nil {
		slog.Error("failed fetch keys", slog.String("bucket", bucket), slog.Any("error", err))
		return nil, err
	}
	return protoResp, nil
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
	store types.Store
}

func (s *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{store: s.store}, nil
}

func (s *RaftFSM) Restore(rc io.ReadCloser) error {
	start := time.Now()
	slog.Info("start to restore items")

	defer func() {
		err := rc.Close()
		if err != nil {
			slog.Error("failed to close reader", slog.Any("error", err))
		}
	}()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		slog.Error("failed to read from snapshot", slog.Any("error", err))
		return err
	}

	keyCount := uint64(0)
	buff := proto.NewBuffer(data)
	for {
		kvp := &protobuf.KeyValuePair{}
		err = buff.DecodeMessage(kvp)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			break // Finished reading all items
		}
		if err != nil {
			slog.Error("failed to decode key-value pair", slog.Any("error", err))
			return err
		}

		// Apply item to the store, including the bucket and key
		if kvp.Bucket == "" {
			slog.Error("bucket name is empty", slog.String("key", kvp.Key))
			return errors.New("bucket name cannot be empty")
		}
		err = s.store.Set(kvp.Bucket, kvp.Key, kvp.Value)
		if err != nil {
			slog.Error("failed to set key-value pair in the store", slog.String("bucket", kvp.Bucket), slog.String("key", kvp.Key), slog.Any("error", err))
			return err
		}

		slog.Debug("restored item", slog.String("bucket", kvp.Bucket), slog.String("key", kvp.Key))
		keyCount++
	}

	slog.Info("finished restoring items", slog.Uint64("count", keyCount), slog.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return nil
}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	start := time.Now()
	slog.Info("start to persist items")

	defer func() {
		if r := recover(); r != nil {
			slog.Error("recovered from panic during persist", slog.Any("reason", r))
			sink.Cancel()
		} else {
			err := sink.Close()
			if err != nil {
				slog.Error("failed to close sink", slog.Any("error", err))
			}
		}
	}()

	ch := s.store.SnapshotItems()
	kvpCount := uint64(0)

	for kvp := range ch {
		if kvp == nil {
			break // Channel closed
		}

		buff := proto.NewBuffer(nil)
		err := buff.EncodeMessage(kvp)
		if err != nil {
			slog.Error("failed to encode key-value pair", slog.String("bucket", kvp.Bucket), slog.String("key", kvp.Key), slog.Any("error", err))
			sink.Cancel()
			return err
		}

		_, err = sink.Write(buff.Bytes())
		if err != nil {
			slog.Error("failed to write key-value pair to snapshot sink", slog.String("bucket", kvp.Bucket), slog.String("key", kvp.Key), slog.Any("error", err))
			sink.Cancel()
			return err
		}

		kvpCount++
	}

	slog.Info("finished persisting items", slog.Uint64("count", kvpCount), slog.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return nil
}

func (s *FSMSnapshot) Release() {
	slog.Info("release")
}
