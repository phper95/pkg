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
}
type Option func(*option)
type Client struct {
	name           string
	urls           []string
	queryLogEnable bool
	username       string
	password       string
	version        int
	bulk           Bulk
}

type Bulk struct {
	name          string
	workers       int
	flushInterval time.Duration
	actionSize    int
	requestSize   int
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
		urls:           urls,
		queryLogEnable: false,
		username:       username,
		password:       password,
		version:        0,
		bulk:           Bulk{},
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
		elastic.SetURL(c.urls...),
		elastic.SetSniff(false),
		elastic.SetBasicAuth(c.username, c.password),
	)
	if err != nil {
		return err
	}

	if c.name == "" {
		c.bulkName = "default-processor"
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
