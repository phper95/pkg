package es

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

var clients map[string]*Client
var mutex sync.Mutex
var EStdLogger stdLogger

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
type option struct {
	MaxOpenConn       int
	MaxIdleConn       int
	ConnMaxLifeSecond time.Duration
	QueryLogEnable    bool
	Bulk              Bulk
}
type Option func(*option)
type Client struct {
	Name           string
	Urls           []string
	QueryLogEnable bool
	Username       string
	password       string
	Version        int
	Bulk           Bulk
	Client         *elastic.Client
	BulkProcessor  *elastic.BulkProcessor
}

type Bulk struct {
	Name          string
	Workers       int
	FlushInterval time.Duration
	ActionSize    int //每批提交的文档数
	RequestSize   int //每批提交的文档大小
	AfterFunc     elastic.BulkAfterFunc
	Ctx           context.Context
}

const (
	SimpleClient = "simple-es-client"
)

func init() {
	EStdLogger = log.New(os.Stdout, "[es] ", log.LstdFlags|log.Lshortfile)
}

func WithQueryLogEnable(enable bool) Option {
	return func(opt *option) {
		opt.QueryLogEnable = enable
	}
}

func WithBulk(bulk Bulk) Option {
	return func(opt *option) {
		opt.Bulk = bulk
	}
}

func InitClient(clientName string, urls []string, username string, password string) error {
	if clients == nil {
		clients = map[string]*Client{}
	}
	client := &Client{
		Urls:           urls,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		Version:        0,
		Bulk:           DefaultBulk(),
	}
	client.Bulk.Name = clientName
	err := client.newClient()
	if err != nil {
		return err
	}
	clients[clientName] = client
	return nil
}

func InitClientWithOptions(clientName string, urls []string, username string, password string, options ...Option) error {
	return nil
}

func InitSimpleClient(urls []string, username, password string) error {
	esClient, err := elastic.NewSimpleClient(elastic.SetURL(urls...), elastic.SetBasicAuth(username, password))
	if err != nil {
		return err
	}
	client := &Client{
		Name:           SimpleClient,
		Urls:           urls,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		Version:        0,
		Bulk:           DefaultBulk(),
	}
	client.Bulk.Name = client.Name
	client.BulkProcessor, err = esClient.BulkProcessor().
		Name(client.Bulk.Name).
		Workers(client.Bulk.Workers).
		BulkActions(client.Bulk.ActionSize).
		BulkSize(client.Bulk.RequestSize).
		FlushInterval(client.Bulk.FlushInterval).
		Stats(true).
		After(client.Bulk.AfterFunc).
		Do(client.Bulk.Ctx)
	if err != nil {
		EStdLogger.Print("init bulkProcessor error ", err)
	}
	clients[SimpleClient] = client
	return nil
}

func GetClient(name string) *Client {
	if client, exist := clients[name]; exist {
		return client
	}
	return nil
}

func GetSimpleClient() *Client {
	if client, exist := clients[SimpleClient]; exist {
		return client
	}
	return nil
}

func (c *Client) newClient() error {
	client, err := elastic.NewClient(
		elastic.SetHealthcheckTimeoutStartup(10*time.Second),
		elastic.SetURL(c.Urls...),
		elastic.SetSniff(false),
		elastic.SetBasicAuth(c.Username, c.password),
	)
	if err != nil {
		return err
	}
	c.Client = client

	if c.Bulk.Name == "" {
		c.Bulk.Name = c.Name
	}

	if c.Bulk.Workers <= 0 {
		c.Bulk.Workers = 1
	}

	//参数合理性校验

	if c.Bulk.RequestSize > 100 {
		EStdLogger.Print("Bulk RequestSize must be smaller than 100MB; it will be ignored.")
		c.Bulk.RequestSize = 100
	}

	if c.Bulk.ActionSize >= 10000 {
		EStdLogger.Print("Bulk ActionSize must be smaller than 10000; it will be ignored.")
		c.Bulk.ActionSize = 10000
	}

	if c.Bulk.FlushInterval >= 60 {
		EStdLogger.Print("Bulk FlushInterval must be smaller than 60s; it will be ignored.")
		c.Bulk.FlushInterval = time.Second * 60
	}
	if c.Bulk.AfterFunc == nil {
		c.Bulk.AfterFunc = defaultBulkFunc
	}
	if c.Bulk.Ctx == nil {
		c.Bulk.Ctx = context.Background()
	}

	c.BulkProcessor, err = c.Client.BulkProcessor().
		Name(c.Bulk.Name).
		Workers(c.Bulk.Workers).
		BulkActions(c.Bulk.ActionSize).
		BulkSize(c.Bulk.RequestSize).
		FlushInterval(c.Bulk.FlushInterval).
		Stats(true).
		After(c.Bulk.AfterFunc).
		Do(c.Bulk.Ctx)
	if err != nil {
		EStdLogger.Print("init bulkProcessor error ", err)
	}
	return nil
}

func defaultBulkFunc(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		EStdLogger.Print("executionId:", executionId, "requests : ", requests, "response:", response, "error", err)
	}

}

func DefaultBulk() Bulk {
	return Bulk{
		Workers:       3,
		FlushInterval: 1,
		ActionSize:    500,
		RequestSize:   5 << 20, // 5 MB,
		AfterFunc:     defaultBulkFunc,
		Ctx:           context.Background(),
	}
}

func CloseAll() {
	for _, c := range clients {
		if c != nil {
			err := c.BulkProcessor.Close()
			if err != nil {
				EStdLogger.Print("bulk close error", err)
			}
		}
	}
}

func (c *Client) Close() error {
	return c.BulkProcessor.Close()
}

func (c *Client) BoolQueryScroll(index []string, typeStr string, query elastic.Query, size int, orderBy []string, orderMethod []bool, highlight *elastic.Highlight, excludeSource []string, routings []string, profile bool, param map[string]interface{}, callback func(map[string]interface{}, map[string]interface{}) bool) error {
	status = true
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

	for {
		res, err := search.Do(context.Background())
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if res == nil {
			logs.Error("nil results !")
			status = false
			break
		}
		if res.Hits == nil {
			logs.Error("expected results.Hits != nil; got nil")
			status = false
			break
		}

		if len(res.Hits.Hits) == 0 {
			break
		}
		if res.TookInMillis >= 500 && profile {
			logs.Warn("latency max search:", res.TookInMillis, index, typeStr, routings, boolQuery)

		}

		for _, hit := range res.Hits.Hits {
			includeIndex := false
			for _, v := range index {
				if v == hit.Index {
					includeIndex = true
					break
				}
			}
			if includeIndex {
				item := make(map[string]interface{})
				err := json.Unmarshal(hit.Source, &item)
				if err != nil {
					logs.Error(err)
					continue
				}

				item["_id"] = hit.Id
				callback(item, param)
			}
		}
	}
	return true
}
