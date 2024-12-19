package types

type Store interface {
	CreateBucket(name string) error
	Set(bucket, key string, value []byte) error
	Get(bucket, key string) []byte
	Delete(bucket, key string) error
	Close() error
}
