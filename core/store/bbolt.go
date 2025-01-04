package store

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/Bl4ck-h00d/stashdb/types"
	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

type BoltStore struct {
	db *bbolt.DB
}

func NewBoltStore(dataDir string) (*BoltStore, error) {
	path := filepath.Join(dataDir, "stash.db")
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, fmt.Errorf("error while opening bolt store: %v", err)
	}
	log.Println("successfully instantiated bolt store")
	return &BoltStore{db: db}, nil
}

func (b *BoltStore) CreateBucket(name string) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("error while creating bucket [%s]: %v", name, err)
	}
	return nil
}

func (b *BoltStore) Set(bucket, key string, value []byte) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		valueWithTimestamp := types.ValueWithTimestamp{Value: value, Timestamp: time.Now().Unix()}
		marshalledData, err := json.Marshal(valueWithTimestamp)
		if err != nil {
			return fmt.Errorf("error while marshalling value: %v", err)
		}
		err = bkt.Put([]byte(key), marshalledData)
		fmt.Println("marshal", string(marshalledData[:]), valueWithTimestamp, string(value))
		return err
	})

	if err != nil {
		return fmt.Errorf("error while setting value for key [%s/%s]: %v", bucket, key, err)
	}

	return nil
}

/*
*
Please note that values returned from Get() are only valid while the transaction is open. If you need to use a value outside of the transaction then you must use copy() to copy it to another byte slice
*/
func (b *BoltStore) Get(bucket, key string) []byte {
	var valueWithTimestamp types.ValueWithTimestamp

	err := b.db.View(func(tx *bolt.Tx) error {
		// Access the bucket
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			return fmt.Errorf("bucket [%s] not found", bucket)
		}

		// Get the raw value
		rawValue := bkt.Get([]byte(key))
		if rawValue == nil {
			return fmt.Errorf("key [%s/%s] not found", bucket, key)
		}

		// Unmarshal the raw value into the struct
		if err := json.Unmarshal(rawValue, &valueWithTimestamp); err != nil {
			return fmt.Errorf("failed to unmarshal value for key [%s/%s]: %v", bucket, key, err)
		}

		return nil
	})

	if err != nil {
		return nil
	}

	// Return the raw value (without the timestamp)
	return valueWithTimestamp.Value
}

func (b *BoltStore) Delete(bucket, key string) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			return fmt.Errorf("bucket [%s] not found", bucket)
		}
		err := bkt.Delete([]byte(key))
		if err != nil {
			return fmt.Errorf("error deleting bucket [%s]: %v", bucket, err)
		}
		return nil
	})
	return err
}

func (b *BoltStore) GetAllBuckets() ([]string, error) {
	var buckets []string
	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			buckets = append(buckets, string(name))
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("error listing the buckets")
	}
	return buckets, nil
}

func (b *BoltStore) GetAllKeys(bucket string, limit int64) (map[string][]byte, error) {
	if limit <= 0 {
		limit = 10 // Default limit
	}

	results := make(map[string][]byte)

	err := b.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			return fmt.Errorf("bucket [%s] not found", bucket)
		}

		// Iterate over keys and values in the bucket
		c := bkt.Cursor()
		var count int64 = 0
		for k, v := c.First(); k != nil; k, v = c.Next() {
			results[string(k)] = v
			count++
			if count >= limit {
				break
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error retrieving key-value pairs from bucket [%s]: %v", bucket, err)
	}

	return results, nil
}

func (b *BoltStore) Close() error {
	err := b.db.Close()
	if err != nil {
		log.Printf("error closing database: %v", err)
	}
	return err
}
