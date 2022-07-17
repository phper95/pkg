package es

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

var clients map[string]*Client
var EStdLogger stdLogger

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
type option struct {
	QueryLogEnable             bool
	GlobalSlowQueryMillisecond int64
	Bulk                       *Bulk
	DebugMode                  bool
	Scheme                     string
}
type Option func(*option)
type Client struct {
	Name           string
	Urls           []string
	QueryLogEnable bool
	Username       string
	password       string
	Bulk           *Bulk
	Client         *elastic.Client
	BulkProcessor  *elastic.BulkProcessor
	DebugMode      bool
	//本地缓存已经创建的索引，用于加速索引是否存在的判断
	CachedIndices sync.Map
	lock          sync.Mutex
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

func WithSlowQueryLogMillisecond(slowQueryMillisecond int64) Option {
	return func(opt *option) {
		opt.GlobalSlowQueryMillisecond = slowQueryMillisecond
	}
}
func WithScheme(scheme string) Option {
	return func(opt *option) {
		opt.Scheme = scheme
	}
}

func WithBulk(bulk *Bulk) Option {
	return func(opt *option) {
		opt.Bulk = bulk
	}
}

func InitClient(clientName string, urls []string, username string, password string) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}
	client := &Client{
		Urls:           urls,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		Bulk:           DefaultBulk(),
		CachedIndices:  sync.Map{},
		lock:           sync.Mutex{},
	}
	client.Bulk.Name = clientName
	options := getBaseOptions(username, password, urls...)
	err := client.newClient(options)
	if err != nil {
		return err
	}
	clients[clientName] = client
	return nil
}

func getBaseOptions(username, password string, urls ...string) []elastic.ClientOptionFunc {
	options := make([]elastic.ClientOptionFunc, 0)
	options = append(options, elastic.SetURL(urls...))
	options = append(options, elastic.SetBasicAuth(username, password))
	options = append(options, elastic.SetHealthcheckTimeoutStartup(15*time.Second))
	//开启Sniff，SDK会定期(默认15分钟一次)嗅探集群中全部节点，将全部节点都加入到连接列表中，
	//后续新增的节点也会自动加入到可连接列表，但实际生产中我们可能会设置专门的协调节点，所以默认不开启嗅探
	options = append(options, elastic.SetSniff(false))
	options = append(options, elastic.SetErrorLog(EStdLogger))
	return options
}

func getDefaultClient() *http.Client {
	tr := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	}
	return &http.Client{Transport: tr}
}

func InitClientWithOptions(clientName string, urls []string, username string, password string, options ...Option) error {
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}
	client := &Client{
		Urls:           urls,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		Bulk:           DefaultBulk(),
		CachedIndices:  sync.Map{},
		lock:           sync.Mutex{},
	}
	opt := &option{}
	for _, f := range options {
		if f != nil {
			f(opt)
		}
	}
	esOptions := getBaseOptions(username, password, urls...)

	if opt.DebugMode {
		esOptions = append(esOptions, elastic.SetInfoLog(EStdLogger))
	}
	if len(opt.Scheme) > 0 {
		esOptions = append(esOptions, elastic.SetScheme(opt.Scheme))
		esOptions = append(esOptions, elastic.SetHttpClient(getDefaultClient()))
		esOptions = append(esOptions, elastic.SetHealthcheck(false))
	}

	client.QueryLogEnable = opt.QueryLogEnable
	client.Bulk = opt.Bulk
	if client.Bulk == nil {
		client.Bulk = DefaultBulk()
	}
	err := client.newClient(esOptions)
	if err != nil {
		return err
	}
	clients[clientName] = client
	return nil
}

func InitSimpleClient(urls []string, username, password string) error {
	esClient, err := elastic.NewSimpleClient(
		elastic.SetURL(urls...),
		elastic.SetBasicAuth(username, password),
		elastic.SetErrorLog(EStdLogger))
	if err != nil {
		return err
	}
	client := &Client{
		Name:           SimpleClient,
		Urls:           urls,
		QueryLogEnable: false,
		Username:       username,
		password:       password,
		Bulk:           DefaultBulk(),
		CachedIndices:  sync.Map{},
		lock:           sync.Mutex{},
	}
	client.Client = esClient
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
	if clients == nil {
		clients = make(map[string]*Client, 0)
	}
	clients[SimpleClient] = client
	return nil
}

func GetClient(name string) *Client {
	if client, exist := clients[name]; exist {
		return client
	}
	EStdLogger.Print("call init", name, "before !!!")
	return nil
}

func GetSimpleClient() *Client {
	if client, exist := clients[SimpleClient]; exist {
		return client
	}
	EStdLogger.Print("call init", SimpleClient, "before !!!")
	return nil
}

func (c *Client) newClient(options []elastic.ClientOptionFunc) error {
	client, err := elastic.NewClient(options...)
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

	if c.Bulk.RequestSize > 100*1024*1024 {
		EStdLogger.Print("Bulk RequestSize must be smaller than 100MB; it will be ignored.")
		c.Bulk.RequestSize = 100 * 1024 * 1024
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
	if err != nil || (response != nil && response.Errors) {
		res, _ := json.Marshal(response)
		EStdLogger.Printf("executionId: %d ;requests : %v; response : %s ; err : %+v", executionId, requests, res, err)
	}

}

func DefaultBulk() *Bulk {
	return &Bulk{
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

func (c *Client) AddIndexCache(indexName ...string) {
	for _, index := range indexName {
		c.CachedIndices.Store(index, true)
	}
}
func (c *Client) DeleteIndexCache(indexName ...string) {
	for _, index := range indexName {
		c.CachedIndices.Delete(index)
	}
}
func (c *Client) Close() error {
	return c.BulkProcessor.Close()
}
