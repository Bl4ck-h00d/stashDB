package store

import (
	"fmt"
	"log"

	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

type BoltStore struct {
	db *bbolt.DB
}

func NewBoltStore(path string) (*BoltStore, error) {
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
		err = bkt.Put([]byte(key), value)
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
	var value []byte
	b.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			return fmt.Errorf("bucket [%s] not found", bucket)
		}
		value = bkt.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("key [%s/%s] not found", bucket, key)
		}
		copiedValue := make([]byte, len(value))
		copy(copiedValue, value)
		value = copiedValue

		return nil
	})
	return value
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

func (b *BoltStore) Close() error {
	err := b.db.Close()
	if err != nil {
		log.Printf("error closing database: %v", err)
	}
	return err
}
