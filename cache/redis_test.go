package cache

import (
	"gitee.com/phper95/pkg/compression"
	"testing"
	"time"
)

type UserTest struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func TestGet(t *testing.T) {
	key := "test"

	user := UserTest{
		ID:   1,
		Name: "imooc",
	}
	userByte, err := compression.MarshalJsonAndGzip(user)
	if err != nil {
		t.Errorf("MarshalJsonAndGzip err %v", err)
	}
	opts := &redis.Options{
		Addr: "127.0.0.1:6379",
	}
	err = InitRedis(DefaultRedisClient, opts, nil)
	if err != nil {
		t.Errorf("InitRedis err %v", err)
	}
	redisClient := GetRedisClient(DefaultRedisClient)
	redisClient.Set(key, userByte, time.Minute)
	val, err := redisClient.Get(key)
	t.Log(val, err)
}
