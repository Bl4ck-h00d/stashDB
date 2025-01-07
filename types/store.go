package types

import "github.com/Bl4ck-h00d/stashdb/protobuf"

type Store interface {
	CreateBucket(name string) error
	Set(bucket, key string, value []byte) error
	Get(bucket, key string) (*ValueWithTimestamp, error)
	Delete(bucket, key string) error
	GetAllBuckets() ([]string, error)
	GetAllKeys(bucket string, limit int64) (map[string]*ValueWithTimestamp, error)
	SnapshotItems() <-chan *protobuf.KeyValuePair
	Close() error
}

type ValueWithTimestamp struct {
	Value     []byte
	Timestamp int64
}

type ValueWithTTL struct {
	Value      []byte
	Timestamp  int64
	Expiration int64
}
