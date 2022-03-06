package cache

import (
	"gitee.com/phper95/pkg/errors"
	"gitee.com/phper95/pkg/timeutil"
	"gitee.com/phper95/pkg/trace"
	"github.com/go-redis/redis/v7"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

var redisClients map[string]Redis

type Redis interface {
	Set(key, value string, ttl time.Duration) error
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

type redisClient struct {
	client        *redis.Client
	clusterClient *redis.ClusterClient
	trace         *trace.Redis
}

const (
	MinIdleConns = 50
	PoolSize     = 20
	MaxRetries   = 3
)

func InitRedis(clientName string, opt *redis.Options, trace *trace.Redis) error {
	if len(clientName) == 0 {
		return errors.New("empty client name")
	}

	if len(opt.Addr) == 0 {
		return errors.New("empty addr")
	}
	client := redis.NewClient(opt)

	if err := client.Ping().Err(); err != nil {
		return errors.Wrap(err, "ping redis err")
	}
	redisClients[clientName] = &redisClient{
		client: client,
		trace:  trace,
	}
	return nil
}

func InitClusterRedis(clientName string, opt *redis.ClusterOptions, trace *trace.Redis) error {
	if len(clientName) == 0 {
		return errors.New("empty client name")
	}
	if len(opt.Addrs) == 0 {
		return errors.New("empty addrs")
	}
	client := redis.NewClusterClient(opt)

	if err := client.Ping().Err(); err != nil {
		return errors.Wrap(err, "ping redis err")
	}
	redisClients[clientName] = &redisClient{
		clusterClient: client,
	}
	return nil
}

func GetRedisClient(name string) Redis {
	if client, ok := redisClients[name]; ok {
		return client
	}
	return nil

}

func GetRedisClusterClient(name string) Redis {
	if client, ok := redisClients[name]; ok {
		return client
	}
	return nil
}

// Set set some <key,value> into redis
func (c *redisClient) Set(key, value string, ttl time.Duration) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}
	ts := time.Now()
	defer func() {
		if c.trace == nil || c.trace.Logger == nil {
			return
		}
		costMillisecond := time.Since(ts).Milliseconds()

		if !c.trace.AlwaysTrace && costMillisecond < c.trace.SlowLoggerMillisecond {
			return
		}
		c.trace.TraceTime = timeutil.CSTLayoutString()
		c.trace.CMD = "set"
		c.trace.Key = key
		c.trace.Value = value
		c.trace.TTL = ttl.Minutes()
		c.trace.CostMillisecond = costMillisecond
		c.trace.Logger.Warn("redis-trace", zap.Any("", c.trace))
	}()

	if c.client != nil {
		if err := c.client.Set(key, value, ttl).Err(); err != nil {
			return errors.Wrapf(err, "redis set key: %s err", key)
		}
		return nil
	}

	//集群版
	if err := c.clusterClient.Set(key, value, ttl).Err(); err != nil {
		return errors.Wrapf(err, "redis set key: %s err", key)
	}
	return nil
}

// Get get some key from redis
func (c *redisClient) Get(key string) (value string, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	ts := time.Now()
	defer func() {
		if c.trace == nil || c.trace.Logger == nil {
			return
		}
		costMillisecond := time.Since(ts).Milliseconds()

		if !c.trace.AlwaysTrace && costMillisecond < c.trace.SlowLoggerMillisecond {
			return
		}
		c.trace.TraceTime = timeutil.CSTLayoutString()
		c.trace.CMD = "get"
		c.trace.Key = key
		c.trace.Value = value
		c.trace.CostMillisecond = costMillisecond
		c.trace.Logger.Warn("redis-trace", zap.Any("", c.trace))
	}()

	if c.client != nil {
		value, err = c.client.Get(key).Result()
		if err != nil {
			return "", errors.Wrapf(err, "redis get key: %s err", key)
		}
		return
	}

	value, err = c.clusterClient.Get(key).Result()
	if err != nil {
		return "", errors.Wrapf(err, "redis get key: %s err", key)
	}
	return
}

// TTL get some key from redis
func (c *redisClient) TTL(key string) (time.Duration, error) {
	if len(key) == 0 {
		return 0, errors.New("empty key")
	}
	if c.client != nil {
		ttl, err := c.client.TTL(key).Result()
		if err != nil {
			return -1, errors.Wrapf(err, "redis get key: %s err", key)
		}
		return ttl, nil
	}
	ttl, err := c.clusterClient.TTL(key).Result()
	if err != nil {
		return -1, errors.Wrapf(err, "redis get key: %s err", key)
	}

	return ttl, nil
}

// Expire expire some key
func (c *redisClient) Expire(key string, ttl time.Duration) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("empty key")
	}
	if c.client != nil {
		ok, err := c.client.Expire(key, ttl).Result()
		return ok, err
	}
	ok, err := c.clusterClient.Expire(key, ttl).Result()
	return ok, err
}

// ExpireAt expire some key at some time
func (c *redisClient) ExpireAt(key string, ttl time.Time) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("empty key")
	}
	if c.client != nil {
		ok, err := c.client.ExpireAt(key, ttl).Result()
		return ok, err
	}
	ok, err := c.clusterClient.ExpireAt(key, ttl).Result()
	return ok, err

}

func (c *redisClient) Exists(keys ...string) (bool, error) {
	if len(keys) == 0 {
		return false, errors.New("empty keys")
	}
	if c.client != nil {
		value, err := c.client.Exists(keys...).Result()
		return value > 0, err
	}
	value, err := c.clusterClient.Exists(keys...).Result()
	return value > 0, err
}

func (c *redisClient) Del(key string) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("empty key")
	}
	ts := time.Now()
	var value int64
	var err error
	defer func() {
		if c.trace == nil || c.trace.Logger == nil {
			return
		}
		costMillisecond := time.Since(ts).Milliseconds()

		if !c.trace.AlwaysTrace && costMillisecond < c.trace.SlowLoggerMillisecond {
			return
		}
		c.trace.TraceTime = timeutil.CSTLayoutString()
		c.trace.CMD = "del"
		c.trace.Key = key
		c.trace.Value = strconv.FormatInt(value, 10)
		c.trace.CostMillisecond = costMillisecond
		c.trace.Logger.Warn("redis-trace", zap.Any("", c.trace))
	}()

	if c.client != nil {
		value, err = c.client.Del(key).Result()
		return value > 0, err
	}

	//集群版
	value, err = c.clusterClient.Del(key).Result()
	return value > 0, err
}

func (c *redisClient) Incr(key string) (value int64, err error) {
	if len(key) == 0 {
		return 0, errors.New("empty key")
	}
	ts := time.Now()
	defer func() {
		if c.trace == nil || c.trace.Logger == nil {
			return
		}
		costMillisecond := time.Since(ts).Milliseconds()

		if !c.trace.AlwaysTrace && costMillisecond < c.trace.SlowLoggerMillisecond {
			return
		}
		c.trace.TraceTime = timeutil.CSTLayoutString()
		c.trace.CMD = "Incr"
		c.trace.Key = key
		c.trace.Value = strconv.FormatInt(value, 10)
		c.trace.CostMillisecond = costMillisecond
		c.trace.Logger.Warn("redis-trace", zap.Any("", c.trace))
	}()
	if c.client != nil {
		value, err = c.client.Incr(key).Result()
		return
	}
	value, err = c.clusterClient.Incr(key).Result()
	return
}

// Close close redis client
func (c *redisClient) Close() error {
	return c.client.Close()
}

// Version redis server version
func (c *redisClient) Version() string {
	if c.client != nil {
		server := c.client.Info("server").Val()
		spl1 := strings.Split(server, "# Server")
		spl2 := strings.Split(spl1[1], "redis_version:")
		spl3 := strings.Split(spl2[1], "redis_git_sha1:")
		return spl3[0]
	}
	server := c.clusterClient.Info("server").Val()
	spl1 := strings.Split(server, "# Server")
	spl2 := strings.Split(spl1[1], "redis_version:")
	spl3 := strings.Split(spl2[1], "redis_git_sha1:")
	return spl3[0]

}
