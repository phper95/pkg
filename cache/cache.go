package cache

import (
	"log"
	"os"
	"time"
)

type Cache interface {
	Set(key string, value interface{}, ttl time.Duration) error
	Get(key string) interface{}
	GetStr(key string) (value string, err error)
	TTL(key string) (time.Duration, error)
	Expire(key string, ttl time.Duration) (bool, error)
	ExpireAt(key string, ttl time.Time) (bool, error)
	Delete(key string) error
	Exists(keys ...string) (bool, error)
	IsExist(key string) bool
	Incr(key string) (int64, error)
	SetBit(key string, offset int64, val int) (value int64, err error)
	GetBit(key string, offset int64) (value int64, err error)
	SetBigBit(key string, offset int64, val int) (value int64, err error)
	GetBigBit(key string, offset int64) (value int64, err error)
	SetBitNOBucket(key string, offset int64, val int) (value int64, err error)
	GetBitNOBucket(key string, offset int64) (value int64, err error)
	BitCountNOBucket(key string, start, end int64) (value int64, err error)
	Close() error
	Version() string
}

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var CacheStdLogger stdLogger

func init() {
	CacheStdLogger = log.New(os.Stdout, "[Cache] ", log.LstdFlags|log.Lshortfile)
}
