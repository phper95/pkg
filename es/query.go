package es

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"io"
)

type Mget struct {
	Index   string
	ID      string
	Routing string
}

func (c *Client) Get(ctx context.Context, indexName, id, routing string) (*elastic.GetResult, error) {
	getService := c.Client.Get().Index(indexName).Id(id)
	if len(routing) > 0 {
		getService.Routing(routing)
	}
	return getService.Do(ctx)
}

func (c *Client) Mget(ctx context.Context, mgetItems []Mget) (*elastic.MgetResponse, error) {
	multiGetService := c.Client.Mget()
	multiGetItems := make([]*elastic.MultiGetItem, 0)
	for _, item := range mgetItems {
		multiGetItem := &elastic.MultiGetItem{}
		multiGetItem.Index(item.Index)
		multiGetItem.Id(item.ID)
		if len(item.Routing) > 0 {
			multiGetItem.Routing(item.Routing)
		}
		multiGetItems = append(multiGetItems, multiGetItem)

	}
	return multiGetService.Add(multiGetItems...).Do(ctx)
}

func (c *Client) ScrollQuery(ctx context.Context, index []string, typeStr string, query elastic.Query, size int, orderBy []string, orderMethod []bool, highlight *elastic.Highlight, excludeSource []string, routings []string, profile bool, callback func(res *elastic.SearchResult, err error)) {
	fetchSourceContext := elastic.NewFetchSourceContext(true)
	searchSource := elastic.NewSearchSource()
	searchSource = searchSource.FetchSourceContext(fetchSourceContext).Query(query)

	if profile {
		searchSource.Profile(profile)
	}

	for i := range orderBy {
		if orderBy[i] != "" {
			searchSource = searchSource.Sort(orderBy[i], orderMethod[i])
		}
	}

	//searchSource = searchSource.From(from).Size(size)
	if highlight != nil {
		searchSource.Highlight(highlight)
	}
	if excludeSource != nil {
		searchSource.FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude(excludeSource...))
	}
	if c.QueryLogEnable {
		// 调试模式打开查询原生log
		src, _ := searchSource.Source()
		data, err := json.Marshal(src)
		EStdLogger.Print(string(data), err)
	}
	search := c.Client.Scroll(index...).SearchSource(searchSource).Pretty(false).Routing(routings...).Size(size)
	//scroll保存在ES集群中的上下文信息会占用大量内存资源，虽然会在一段时间后自动清理，当我们知道scroll结束后,
	//需要手动调用clear释放资源
	defer search.Clear(ctx)
	for {
		res, err := search.Do(ctx)
		if err == io.EOF {
			break
		}

		if res == nil {
			EStdLogger.Print("nil results !")
			break
		}
		if res.Hits == nil {
			EStdLogger.Print("expected results.Hits != nil; got nil")
		}

		if res.TookInMillis >= 500 && profile {
			EStdLogger.Print("latency max search:", res.TookInMillis, index, typeStr, routings, query)
		}

		if len(res.Hits.Hits) == 0 {
			break
		}
		callback(res, err)
	}
}
