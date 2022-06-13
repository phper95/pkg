package es

import (
	"context"
)

//检查索引是否存在
func (c *Client) IndexExists(ctx context.Context, indexName string, forceCheck bool) (bool, error) {
	//如果需要强制校验，就不走索引缓存，用于一些自动清理的索引或者可能会被其他应用或者人为删掉的情况，
	//这种情况下没法同步本地缓存
	if !forceCheck {
		if _, ok := c.CachedIndices.Load(indexName); ok {
			return true, nil
		}
	}
	//在ES中可以同时校验多个索引是否存在，校验多个索引时，
	//只要有一个索引不存在，就会返回false，实际场景很少会用到，这里直接校验单索引
	exists, err := c.Client.IndexExists(indexName).Do(ctx)
	if exists {
		c.CachedIndices.Store(indexName, true)
	}
	return exists, err

}

//创建索引
func (c *Client) CreateIndex(ctx context.Context, indexName, bodyJson string, forceCheck bool) error {
	//创建索引比较耗时，创建过程中防止其他创建请求过来，这里可以加锁处理
	c.lock.Lock()
	defer c.lock.Unlock()
	exist, err := c.IndexExists(ctx, indexName, forceCheck)
	if err != nil {
		return err
	}
	// 索引已创建
	if exist {
		return nil
	}
	// 如果重复创建会报错
	_, err = c.Client.CreateIndex(indexName).BodyString(bodyJson).Do(ctx)
	if err != nil {
		c.CachedIndices.Store(indexName, true)
	}
	return err
}
