package types

type Store interface {
	CreateBucket(name string) error
	Set(bucket, key string, value []byte) error
	Get(bucket, key string) []byte
	Delete(bucket, key string) error
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
