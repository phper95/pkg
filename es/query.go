package es

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"io"
	"strings"
)

type Mget struct {
	Index   string
	ID      string
	Routing string
}
type queryOption struct {
	Orders               map[string]bool
	Highlight            *elastic.Highlight
	Profile              bool
	EnableDSL            bool
	ExcludeFields        []string
	IncludeFields        []string
	SlowQueryMillisecond int64
}
type QueryOption func(queryOption *queryOption)

func WithOrders(orders map[string]bool) QueryOption {
	return func(opt *queryOption) {
		opt.Orders = orders
	}
}
func WithHighlight(highlight *elastic.Highlight) QueryOption {
	return func(opt *queryOption) {
		opt.Highlight = highlight
	}
}

func WithProfile(profile bool) QueryOption {
	return func(opt *queryOption) {
		opt.Profile = profile
	}
}

func WithEnableDSL(enableDSL bool) QueryOption {
	return func(opt *queryOption) {
		opt.EnableDSL = enableDSL
	}
}
func WithIncludeFields(includeFields []string) QueryOption {
	return func(opt *queryOption) {
		opt.IncludeFields = includeFields
	}
}

func WithExcludeFields(excludeFields []string) QueryOption {
	return func(opt *queryOption) {
		opt.ExcludeFields = excludeFields
	}
}
func WithSlowQueryMillisecond(slowQueryLogMillisecond int64) QueryOption {
	return func(opt *queryOption) {
		opt.SlowQueryMillisecond = slowQueryLogMillisecond
	}
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
func (c *Client) Query(ctx context.Context, indexName string, routings []string, query elastic.Query, from, size int, options ...QueryOption) (*elastic.SearchResult, error) {
	queryOpt := &queryOption{}
	for _, f := range options {
		if f != nil {
			f(queryOpt)
		}
	}

	//设置Source
	fetchSourceContext := elastic.NewFetchSourceContext(true)
	if len(queryOpt.IncludeFields) > 0 {
		fetchSourceContext.Include(queryOpt.IncludeFields...)
	}
	if len(queryOpt.ExcludeFields) > 0 {
		fetchSourceContext.Exclude(queryOpt.ExcludeFields...)
	}

	//构造查询条件
	searchSource := elastic.NewSearchSource()
	searchSource = searchSource.FetchSourceContext(fetchSourceContext).Query(query).From(from).Size(size)
	if len(queryOpt.Orders) > 0 {
		for field, order := range queryOpt.Orders {
			searchSource.Sort(field, order)
		}
	}
	if queryOpt.Highlight != nil {
		searchSource.Highlight(queryOpt.Highlight)
	}
	searchSource.Profile(queryOpt.Profile)

	searchService := c.Client.Search(indexName).SearchSource(searchSource).IgnoreUnavailable(true)
	if len(routings) > 0 {
		searchService.Routing(routings...)
	}
	res, err := searchService.Do(ctx)
	//获取查询语句
	src, _ := searchSource.Source()
	data, _ := json.Marshal(src)
	rs := strings.Join(routings, ",")
	if c.DebugMode || c.QueryLogEnable || queryOpt.EnableDSL {
		EStdLogger.Print("DSL : ", string(data), "routing: ", rs)
	}
	if queryOpt.SlowQueryMillisecond > 0 && res != nil && res.TookInMillis >= queryOpt.SlowQueryMillisecond {
		EStdLogger.Print("slow query DSL : ", string(data), "routing: ", rs)
	}
	return res, err

}
func (c *Client) ScrollQuery(ctx context.Context, index []string, typeStr string, query elastic.Query, size int, routings []string, callback func(res *elastic.SearchResult, err error), options ...QueryOption) {
	queryOpt := &queryOption{}
	for _, f := range options {
		if f != nil {
			f(queryOpt)
		}
	}
	fetchSourceContext := elastic.NewFetchSourceContext(true)
	searchSource := elastic.NewSearchSource()
	searchSource = searchSource.FetchSourceContext(fetchSourceContext).Query(query)

	if len(queryOpt.Orders) > 0 {
		for field, order := range queryOpt.Orders {
			searchSource.Sort(field, order)
		}
	}
	if len(queryOpt.Orders) > 0 {
		for field, order := range queryOpt.Orders {
			searchSource.Sort(field, order)
		}
	}
	if queryOpt.Highlight != nil {
		searchSource.Highlight(queryOpt.Highlight)
	}
	searchSource.Profile(queryOpt.Profile)
	src, _ := searchSource.Source()
	data, _ := json.Marshal(src)
	rs := strings.Join(routings, ",")
	if c.DebugMode || c.QueryLogEnable || queryOpt.EnableDSL {
		EStdLogger.Print("DSL : ", string(data), "routing: ", rs)
	}
	scrollService := c.Client.Scroll(index...).SearchSource(searchSource).Size(size)
	if len(routings) > 0 {
		scrollService.Routing(routings...)
	}
	//scroll保存在ES集群中的上下文信息会占用大量内存资源，虽然会在一段时间后自动清理，当我们知道scroll结束后,
	//需要手动调用clear释放资源
	defer scrollService.Clear(ctx)
	for {
		res, err := scrollService.Do(ctx)
		if err == io.EOF {
			break
		}
		if queryOpt.SlowQueryMillisecond > 0 && res != nil && res.TookInMillis >= queryOpt.SlowQueryMillisecond {
			EStdLogger.Print("slow query DSL : ", string(data), "routing: ", rs)
		}
		if res == nil {
			EStdLogger.Print("nil results !")
			break
		}
		if res.Hits == nil {
			EStdLogger.Print("expected results.Hits != nil; got nil")
		}

		if res.TookInMillis >= 500 {
			EStdLogger.Print("latency max search:", res.TookInMillis, index, typeStr, routings, query)
		}

		if len(res.Hits.Hits) == 0 {
			break
		}
		callback(res, err)
	}
}
