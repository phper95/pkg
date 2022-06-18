package cache

import "time"

type Cache interface {
	Set(key, value interface{}, ttl time.Duration) error
	Get(key string) (value string, err error)
	TTL(key string) (time.Duration, error)
	Expire(key string, ttl time.Duration) (bool, error)
	ExpireAt(key string, ttl time.Time) (bool, error)
	Del(key string) (bool, error)
	Exists(keys ...string) (bool, error)
	Incr(key string) (int64, error)
	Close() error
	Version() string
}
