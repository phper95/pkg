package cache

import (
	"gitee.com/phper95/pkg/compression"
	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/assert"
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
	val, _ := redisClient.GetStr(key)
	output := UserTest{}
	err = compression.UnmarshalDataFromJsonWithGzip([]byte(val), &output)
	if err != nil {
		t.Error("UnmarshalDataFromJsonWithGzip error", val, err)
	}
	t.Log(output)
}

func TestBitOp(t *testing.T) {
	opts := &redis.Options{
		Addr: "127.0.0.1:6379",
	}
	//op计算后的结果集
	key := "dest-bit"

	//参与计算第一个key
	key1 := "test-bit1"

	////参与计算第二个key
	key2 := "test-bit2"

	err := InitRedis(DefaultRedisClient, opts, nil)
	if err != nil {
		t.Errorf("InitRedis err %v", err)
	}
	redisClient := GetRedisClient(DefaultRedisClient)
	redisClient.SetBitNOBucket(key1, 0, 1)
	redisClient.SetBitNOBucket(key1, 1, 1)
	redisClient.SetBitNOBucket(key2, 0, 1)
	redisClient.SetBitNOBucket(key2, 1, 1)
	res, err := redisClient.BitOPNOBucket("and", key, key1, key2)
	if err != nil {
		t.Errorf("BitOPNOBucket err %v", err)
	}
	count, err := redisClient.BitCountNOBucket(key, 0, -1)
	if err != nil {
		t.Errorf("BitCountNOBucket err %v", err)
	}
	if !assert.EqualValues(t, count, 2) {
		t.Error("count except 2 but ", count)
	}
	err = redisClient.Delete(key)
	t.Log(res, err)
}
