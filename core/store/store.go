package store


type Store interface {
	CreateBucket(name string) error
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	Close() error	
}
