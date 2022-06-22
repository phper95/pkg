package cache

import (
	"fmt"
	"gitee.com/phper95/pkg/errors"
	"gitee.com/phper95/pkg/timeutil"
	"go.uber.org/zap"
	"time"
)

//对于超出redis bitmap范围的数据我们使用高49位作捅，低15为作offset

//高49位作捅，低15为作offset
func GetBigBucket(ID int64) int64 {
	return ID >> 15
}

//0x7FFF的二进制为111111111111111
//与ID做与运算结果保留了ID的低15位
func GetBigOffset(ID int64) int64 {
	return ID & 0x7FFF
}

//对于redis bitmap范围内的数据，使用高16位作捅，低16位作offset
func GetBucket(userID int64) int64 {
	return userID >> 16
}

func GetOffset(ID int64) int64 {
	return ID & 0xFFFF
}

func GetKey(key string, ID int64) string {
	return fmt.Sprintf("%s_%d", key, GetBucket(ID))
}

func GetBigKey(key string, ID int64) string {
	return fmt.Sprintf("%s_%d", key, GetBigBucket(ID))
}

func (r *Redis) GetBit(key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	ts := time.Now()
	//集群版为了避免单个bitmap只会落到集群中的一个节点，这里默认对bitmap进行分捅，以平衡redis集群负载，防止单个bitmap热点问题
	realKey := GetKey(key, offset)

	defer func() {
		if r.trace == nil || r.trace.Logger == nil {
			return
		}
		costMillisecond := time.Since(ts).Milliseconds()

		if !r.trace.AlwaysTrace && costMillisecond < r.trace.SlowLoggerMillisecond {
			return
		}
		r.trace.TraceTime = timeutil.CSTLayoutString()
		r.trace.CMD = "getbit"
		r.trace.Key = realKey
		r.trace.Value = fmt.Sprintf("origin : %d ; real: %d ", GetOffset(offset))
		r.trace.CostMillisecond = costMillisecond
		r.trace.Logger.Warn("redis-trace", zap.Any("", r.trace))
	}()

	if r.client != nil {
		value, err = r.client.GetBit(realKey, GetOffset(offset)).Result()
		if err != nil {
			return value, errors.Wrapf(err, "redis getbit key: %s err", key)
		}
		return
	}

	value, err = r.clusterClient.GetBit(realKey, GetOffset(offset)).Result()
	if err != nil {
		return value, errors.Wrapf(err, "redis getbit key: %s err", realKey)
	}
	return
}

func (r *Redis) GetBigBit(key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	ts := time.Now()

	//为了避免过大的offset导致读取性能的问题，这里需要分桶存储
	realKey := GetKey(key, offset)
	defer func() {
		if r.trace == nil || r.trace.Logger == nil {
			return
		}
		costMillisecond := time.Since(ts).Milliseconds()

		if !r.trace.AlwaysTrace && costMillisecond < r.trace.SlowLoggerMillisecond {
			return
		}
		r.trace.TraceTime = timeutil.CSTLayoutString()
		r.trace.CMD = "getbit"
		r.trace.Key = realKey
		r.trace.Value = fmt.Sprintf("origin : %d ; real: %d ", GetBigOffset(offset))
		r.trace.CostMillisecond = costMillisecond
		r.trace.Logger.Warn("redis-trace", zap.Any("", r.trace))
	}()

	if r.client != nil {
		value, err = r.client.GetBit(realKey, GetOffset(offset)).Result()
		if err != nil {
			return value, errors.Wrapf(err, "redis getbit key: %s err", realKey)
		}
		return
	}

	//集群版为了避免单个bitmap只会落到集群中的一个节点，这里默认对bitmap进行分捅，以平衡redis集群负载，防止单个bitmap热点问题
	//对于超过redis bitmap范围的数据，采用不同的分捅方式

	value, err = r.clusterClient.GetBit(realKey, GetBigOffset(offset)).Result()
	if err != nil {
		return value, errors.Wrapf(err, "redis getbit key: %s err", realKey)
	}
	return
}

func (r *Redis) SetBit(key string, offset int64, val int) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	ts := time.Now()

	//为了避免过大的offset导致读取性能的问题，这里需要分桶存储
	realKey := GetKey(key, offset)
	defer func() {
		if r.trace == nil || r.trace.Logger == nil {
			return
		}
		costMillisecond := time.Since(ts).Milliseconds()

		if !r.trace.AlwaysTrace && costMillisecond < r.trace.SlowLoggerMillisecond {
			return
		}
		r.trace.TraceTime = timeutil.CSTLayoutString()
		r.trace.CMD = "setbit"
		r.trace.Key = realKey
		r.trace.Value = val
		r.trace.CostMillisecond = costMillisecond
		r.trace.Logger.Warn("redis-trace", zap.Any("", r.trace))
	}()

	if r.client != nil {
		value, err = r.client.SetBit(realKey, GetOffset(offset), val).Result()
		if err != nil {
			return value, errors.Wrapf(err, "redis setbit key: %s err", realKey)
		}
		return
	}

	//集群版为了避免单个bitmap只会落到集群中的一个节点，这里默认对bitmap进行分捅，以平衡redis集群负载，防止单个bitmap热点问题

	value, err = r.clusterClient.SetBit(realKey, GetOffset(offset), val).Result()
	if err != nil {
		return value, errors.Wrapf(err, "redis setbit key: %s err", realKey)
	}
	return
}

func (r *Redis) SetBigBit(key string, offset int64, val int) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	ts := time.Now()

	//为了避免过大的offset导致读取性能的问题，这里需要分桶存储
	realKey := GetBigKey(key, offset)
	defer func() {
		if r.trace == nil || r.trace.Logger == nil {
			return
		}
		costMillisecond := time.Since(ts).Milliseconds()

		if !r.trace.AlwaysTrace && costMillisecond < r.trace.SlowLoggerMillisecond {
			return
		}
		r.trace.TraceTime = timeutil.CSTLayoutString()
		r.trace.CMD = "setbit"
		r.trace.Key = realKey
		r.trace.Value = val
		r.trace.CostMillisecond = costMillisecond
		r.trace.Logger.Warn("redis-trace", zap.Any("", r.trace))
	}()

	if r.client != nil {
		value, err = r.client.SetBit(realKey, GetBigOffset(offset), val).Result()
		if err != nil {
			return value, errors.Wrapf(err, "redis setbit key: %s err", realKey)
		}
		return
	}

	value, err = r.clusterClient.SetBit(realKey, GetBigOffset(offset), val).Result()
	if err != nil {
		return value, errors.Wrapf(err, "redis setbit key: %s err", realKey)
	}
	return
}
