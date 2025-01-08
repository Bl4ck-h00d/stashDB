package store

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/Bl4ck-h00d/stashdb/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelStore struct {
	db *leveldb.DB
}

func NewLevelStore(dataDir string) (*LevelStore, error) {
	db, err := leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return nil, fmt.Errorf("error while opening LevelDB store: %v", err)
	}
	log.Println("successfully instantiated LevelDB store")
	return &LevelStore{db: db}, nil
}

func (l *LevelStore) CreateBucket(name string) error {
	// LevelDB doesn't support buckets directly. You can use prefixes to simulate them.
	return nil
}

func (l *LevelStore) Set(bucket, key string, value []byte) error {
	valueWithTimestamp := types.ValueWithTimestamp{Value: value, Timestamp: time.Now().Unix()}
	marshalledData, err := json.Marshal(valueWithTimestamp)
	if err != nil {
		return fmt.Errorf("error while marshalling value: %v", err)
	}

	err = l.db.Put([]byte(bucket+"_"+key), marshalledData, nil)
	if err != nil {
		return fmt.Errorf("error while setting value for key [%s/%s]: %v", bucket, key, err)
	}

	return nil
}

func (l *LevelStore) Get(bucket, key string) (*types.ValueWithTimestamp, error) {
	data, err := l.db.Get([]byte(bucket+"_"+key), nil)
	if err != nil {
		return nil, fmt.Errorf("error getting key [%s/%s]: %v", bucket, key, err)
	}

	var valueWithTimestamp types.ValueWithTimestamp
	if err := json.Unmarshal(data, &valueWithTimestamp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal value for key [%s/%s]: %v", bucket, key, err)
	}

	return &valueWithTimestamp, nil
}

func (l *LevelStore) Delete(bucket, key string) error {
	err := l.db.Delete([]byte(bucket+"_"+key), nil)
	if err != nil {
		return fmt.Errorf("error deleting key [%s/%s]: %v", bucket, key, err)
	}
	return nil
}

func (l *LevelStore) GetAllBuckets() ([]string, error) {
	// LevelDB does not have real buckets, so this would require scanning with prefixes, which is not directly supported.
	return nil, fmt.Errorf("GetAllBuckets is not supported in LevelDB as it does not have bucket functionality")
}

func (l *LevelStore) GetAllKeys(bucket string, limit int64) (map[string]*types.ValueWithTimestamp, error) {
	results := make(map[string]*types.ValueWithTimestamp)

	// Iterate through all keys with the given bucket prefix
	iter := l.db.NewIterator(&util.Range{Start: []byte(bucket)}, nil)
	defer iter.Release()

	var count int64
	for iter.Next() {
		key := string(iter.Key())
		if len(key) > len(bucket) && key[:len(bucket)] == bucket {
			var valueWithTimestamp types.ValueWithTimestamp
			if err := json.Unmarshal(iter.Value(), &valueWithTimestamp); err != nil {
				return nil, fmt.Errorf("failed to unmarshal value for key [%s]: %v", key, err)
			}

			results[key] = &valueWithTimestamp
			count++
			if count >= limit {
				break
			}
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("error iterating keys: %v", err)
	}

	return results, nil
}

func (l *LevelStore) SnapshotItems() <-chan *protobuf.KeyValuePair {
	ch := make(chan *protobuf.KeyValuePair)

	go func() {
		defer close(ch)

		iter := l.db.NewIterator(nil, nil)
		defer iter.Release()

		for iter.Next() {
			key := string(iter.Key())
			var valueWithTimestamp types.ValueWithTimestamp
			if err := json.Unmarshal(iter.Value(), &valueWithTimestamp); err != nil {
				log.Printf("failed to unmarshal value for key [%s]: %v", key, err)
				continue
			}

			ch <- &protobuf.KeyValuePair{
				Bucket: "default", // Since LevelDB does not have buckets, use a placeholder
				Key:    key,
				Value:  valueWithTimestamp.Value,
			}
		}
	}()

	return ch
}

func (l *LevelStore) Close() error {
	return l.db.Close()
}
