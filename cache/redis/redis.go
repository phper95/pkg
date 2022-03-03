package redis

import (
	"github.com/go-redis/redis/v7"
	"pkg/cache"
	"pkg/errors"
	"pkg/timeutil"
	"pkg/trace"
	"strings"
	"time"
)

type redisClient struct {
	client *redis.Client
}

var redisClients = make(map[string]*redisClient)

func InitRedis(clientName string, option *cache.CacheOption) error {
	client, err := redisConnect(option)
	redisClients[clientName] = &redisClient{
		client: client,
	}
	return err
}
func GetRedisClient(clientName string) *redisClient {
	return redisClients[clientName]
}

func redisConnect(option *cache.CacheOption) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         option.Addrs,
		Password:     option.Password,
		DB:           option.DB,
		MaxRetries:   option.MaxRetries,
		PoolSize:     option.PoolSize,
		MinIdleConns: option.MinIdleConns,
	})

	if err := client.Ping().Err(); err != nil {
		return nil, errors.Wrap(err, "ping redis err")
	}

	return client, nil
}

// Set set some <key,value> into redis
func (c *redisClient) Set(key, value string, ttl time.Duration, trace *trace.Cache) error {
	ts := time.Now()

	defer func() {
		CostMillisecond := time.Since(ts).Milliseconds()
		if trace.Open || CostMillisecond >= trace.SlowWarnLogMillisecond {
			trace.TraceTime = timeutil.CSTLayoutString()
			trace.CMD = "set"
			trace.Key = key
			trace.Value = value
			trace.TTL = ttl.Minutes()
			trace.CostMillisecond = CostMillisecond
			trace.Logger.Warn()
		}
	}()

	for _, f := range options {
		f(opt)
	}

	if err := c.client.Set(key, value, ttl).Err(); err != nil {
		return errors.Wrapf(err, "redis set key: %s err", key)
	}

	return nil
}

// Get get some key from redis
func (c *redisClient) Get(key string, trace trace.Cache) (string, error) {
	ts := time.Now()
	defer func() {
		if trace.Trace != nil {
			opt.Redis.Timestamp = timeutil.CSTLayoutString()
			opt.Redis.Handle = "get"
			opt.Redis.Key = key
			opt.Redis.CostSeconds = time.Since(ts).Seconds()
			opt.Trace.AppendRedis(opt.Redis)
		}
	}()

	for _, f := range options {
		f(opt)
	}

	value, err := c.client.Get(key).Result()
	if err != nil {
		return "", errors.Wrapf(err, "redis get key: %s err", key)
	}

	return value, nil
}

// TTL get some key from redis
func (c *redisClient) TTL(key string) (time.Duration, error) {
	ttl, err := c.client.TTL(key).Result()
	if err != nil {
		return -1, errors.Wrapf(err, "redis get key: %s err", key)
	}

	return ttl, nil
}

// Expire expire some key
func (c *redisClient) Expire(key string, ttl time.Duration) bool {
	ok, _ := c.client.Expire(key, ttl).Result()
	return ok
}

// ExpireAt expire some key at some time
func (c *redisClient) ExpireAt(key string, ttl time.Time) bool {
	ok, _ := c.client.ExpireAt(key, ttl).Result()
	return ok
}

func (c *redisClient) Exists(keys ...string) bool {
	if len(keys) == 0 {
		return true
	}
	value, _ := c.client.Exists(keys...).Result()
	return value > 0
}

func (c *redisClient) Del(key string, trace trace.Cache) bool {
	ts := time.Now()

	defer func() {
		if opt.Trace != nil {
			opt.Redis.Timestamp = timeutil.CSTLayoutString()
			opt.Redis.Handle = "del"
			opt.Redis.Key = key
			opt.Redis.CostSeconds = time.Since(ts).Seconds()
			opt.Trace.AppendRedis(opt.Redis)
		}
	}()

	for _, f := range options {
		f(opt)
	}

	if key == "" {
		return true
	}

	value, _ := c.client.Del(key).Result()
	return value > 0
}

func (c *redisClient) Incr(key string, trace trace.Cache) int64 {
	ts := time.Now()

	defer func() {
		if opt.Trace != nil {
			opt.Redis.Timestamp = timeutil.CSTLayoutString()
			opt.Redis.Handle = "incr"
			opt.Redis.Key = key
			opt.Redis.CostSeconds = time.Since(ts).Seconds()
			opt.Trace.AppendRedis(opt.Redis)
		}
	}()

	for _, f := range options {
		f(opt)
	}
	value, _ := c.client.Incr(key).Result()
	return value
}

// Close close redis client
func (c *redisClient) Close() error {
	return c.client.Close()
}

// WithTrace 设置trace信息
func WithTrace(t Trace) Option {
	return func(opt *option) {
		if t != nil {
			opt.Trace = t.(*trace.Trace)
			opt.Redis = new(trace.Redis)
		}
	}
}

// Version redis server version
func (c *redisClient) Version() string {
	server := c.client.Info("server").Val()
	spl1 := strings.Split(server, "# Server")
	spl2 := strings.Split(spl1[1], "redis_version:")
	spl3 := strings.Split(spl2[1], "redis_git_sha1:")
	return spl3[0]
}
