package cache

import (
	"pkg/trace"
	"time"
)

type CacheOption struct {
	Addrs        string
	Password     string
	DB           int
	MaxRetries   int
	PoolSize     int
	MinIdleConns int
}

type ICache interface {
	Set(key, value string, ttl time.Duration, trace trace.Cache) error
	Get(key string, trace trace.Cache) (string, error)
	TTL(key string) (time.Duration, error)
	Expire(key string, ttl time.Duration) bool
	ExpireAt(key string, ttl time.Time) bool
	Del(key string, trace trace.Cache) bool
	Exists(keys ...string) bool
	Incr(key string, trace trace.Cache) int64
	Close() error
	Version() string
}
