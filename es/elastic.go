package es

import (
	"github.com/olivere/elastic/v7"
	"sync"
	"time"
)

var clients map[string]*Client
var mutex sync.Mutex

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
}

type Bulk struct {
	Name          string
	Workers       int
	FlushInterval time.Duration
	ActionSize    int //每批提交的文档数
	RequestSize   int //每批提交的文档大小
}

func WithQueryLogEnable(enable bool) Option {
	return func(opt *option) {
		opt.QueryLogEnable = enable
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
		Bulk: Bulk{
			Name:          clientName,
			Workers:       3,
			FlushInterval: 1,
			ActionSize:    500,
			RequestSize:   5 << 20, // 5 MB,
		},
	}
	err := client.init()
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
	elastic.NewSimpleClient(elastic.SetURL(urls...), elastic.SetBasicAuth(username, password))
}

func GetClient(name string) *Client {
	if client, exist := clients[name]; exist {
		return client
	}
	return nil
}

func (c *Client) init() error {
	temp, err := elastic.NewClient(
		elastic.SetHealthcheckTimeoutStartup(10*time.Second),
		elastic.SetURL(c.Urls...),
		elastic.SetSniff(false),
		elastic.SetBasicAuth(c.Username, c.password),
	)
	if err != nil {
		return err
	}

	if c.Bulk.Name == "" {
		c.Bulk.Name = c.Name
	}

	if c.bulkWorkers <= 0 {
		c.bulkWorkers = 1
	}

	if c.bulkRequestSize == 0 {
		c.bulkRequestSize = -1
	}

	if c.bulkActionSize == 0 {
		c.bulkActionSize = -1
	}

	if c.bulkFlushInterval == 0 {
		c.bulkFlushInterval = time.Second
	}

	c.inited = true
	c.esClient = temp
	c.bulkProcessor, err = c.esClient.BulkProcessor().
		Name(c.bulkName).
		Workers(c.bulkWorkers).
		BulkActions(c.bulkActionSize).
		BulkSize(c.bulkRequestSize).
		FlushInterval(c.bulkFlushInterval).
		Stats(true).
		After(c.bulkAfterFunc).
		Do(context.Background())
	if err != nil {
		logs.Error("####### BulkProcessor err ", err)
	}
	return nil
}
