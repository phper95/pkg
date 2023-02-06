package es

import (
	"context"
	"github.com/olivere/elastic/v7"
)

const (
	DefaultClient       = "es-default-client"
	DefaultReadClient   = "es-default-read-client"
	DefaultWriteClient  = "es-default-write-client"
	DefaultVersionType  = "external"
	VersionTypeInternal = "internal"
	DefaultRefresh      = "false"
	RefreshWaitFor      = "wait_for"
	RefreshTrue         = "true"
	DefaultScriptLang   = "painless"
)

type BulkDoc struct {
	ID          string
	Routing     string
	Version     int64
	VersionType string
}
type BulkCreateDoc struct {
	BulkDoc
	Doc interface{}
}

type BulkUpsertDoc struct {
	BulkCreateDoc
	update map[string]interface{}
}
type BulkUpdateDoc struct {
	BulkDoc
	update map[string]interface{}
}

// 新建文档
func (c *Client) Create(ctx context.Context, indexName, id, routing string, doc interface{}) error {
	//注意 sdk这里第一个index获取的是*IndexService，即索引服务，第二个index是指定需要写入的索引名
	indexService := c.Client.Index().Index(indexName).OpType("create")
	if len(id) > 0 {
		indexService.Id(id)
	}
	if len(routing) > 0 {
		indexService.Routing(routing)
	}
	//Refresh setting
	//false , defaule value;不采取任何与刷新有关的行动。这个请求所做的改变将在请求返回后的某个时间点变得可见
	//Empty string or true; 在操作发生后立即刷新相关的主分片和副本分片（而不是整个索引），以便更新的文档立即出现在搜索结果中.对性能影响最大
	//wait_for 在操作响应之前，等待请求所做的改变通过刷新而变得可见。这并不强迫立即进行刷新，而是等待刷新的发生。Elasticsearch每隔index.refresh_interval（默认为一秒）就会自动刷新发生变化的分片
	_, err := indexService.BodyJson(doc).Refresh(DefaultRefresh).Do(ctx)
	return err
}

// 批量的方式新建文档，后台提交
func (c *Client) BulkCreate(indexName, id, routing string, doc interface{}) {
	bulkCreateRequest := elastic.NewBulkCreateRequest().Index(indexName).Doc(doc)
	if len(id) > 0 {
		bulkCreateRequest.Id(id)
	}
	if len(routing) > 0 {
		bulkCreateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkCreateRequest)
}

// 批量的方式新建一批文档,实时提交
func (c *Client) BulkCreateDocs(ctx context.Context, indexName string, docs []*BulkCreateDoc) (*elastic.BulkResponse, error) {
	bulkService := c.Client.Bulk().ErrorTrace(true)
	for _, doc := range docs {
		bulkCreateRequest := elastic.NewBulkCreateRequest().Index(indexName)
		if len(doc.ID) > 0 {
			bulkCreateRequest.Id(doc.ID)
		}
		if len(doc.Routing) > 0 {
			bulkCreateRequest.Routing(doc.Routing)
		}

		bulkService.Add(bulkCreateRequest)
	}
	return bulkService.Do(ctx)
}

func (c *Client) BulkReplace(indexName, id, routing string, doc interface{}) {
	bulkCreateRequest := elastic.NewBulkIndexRequest().Index(indexName).Doc(doc)
	if len(id) > 0 {
		bulkCreateRequest.Id(id)
	}
	if len(routing) > 0 {
		bulkCreateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkCreateRequest)
}

func (c *Client) BulkReplaceDocs(ctx context.Context, indexName string, docs []*BulkCreateDoc) (*elastic.BulkResponse, error) {
	bulkService := c.Client.Bulk().ErrorTrace(true)
	for _, doc := range docs {
		bulkCreateRequest := elastic.NewBulkIndexRequest().Index(indexName)
		if len(doc.ID) > 0 {
			bulkCreateRequest.Id(doc.ID)
		}
		if len(doc.Routing) > 0 {
			bulkCreateRequest.Routing(doc.Routing)
		}
		bulkService.Add(bulkCreateRequest)
	}
	return bulkService.Do(ctx)
}

// 业务数据修改时生成版本号，在更新ES文档数据时，根据这个版本号来避免重复和乱序处理，实现操作的幂等性
// PUT twitter/_doc/1?version=2&version_type=external
// {
// "message" : "elasticsearch now has versioning support, double cool!"
// }
// 1.版本管理是完全实时的，不受搜索操作的近实时方面的影响
// 2.如果当前提供的版本号比实际的版本号大（或者实际文档不存在）就会成功写入，并设置为当前的版本号，否则返回失败（409状态码）
func (c *Client) BulkCreateWithVersion(ctx context.Context, indexName, id, routing string, version int64, doc interface{}) {
	bulkCreateRequest := elastic.NewBulkIndexRequest().Index(indexName).Doc(doc).VersionType(DefaultVersionType).Version(version)
	if len(id) > 0 {
		bulkCreateRequest.Id(id)
	}
	if len(routing) > 0 {
		bulkCreateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkCreateRequest)
}

func (c *Client) Delete(ctx context.Context, indexName, id, routing string) error {
	deleteService := c.Client.Delete().Index(indexName).Id(id).Refresh(DefaultRefresh)
	if len(routing) > 0 {
		deleteService.Routing(routing)
	}
	_, err := deleteService.Do(ctx)
	return err
}

func (c *Client) DeleteRefresh(ctx context.Context, indexName, id, routing string) error {
	deleteService := c.Client.Delete().Index(indexName).Id(id).Refresh(RefreshTrue)
	if len(routing) > 0 {
		deleteService.Routing(routing)
	}
	_, err := deleteService.Do(ctx)
	return err
}

func (c *Client) DeleteWithVersion(ctx context.Context, indexName, id, routing string, version int64) error {
	deleteService := c.Client.Delete().Index(indexName).VersionType(DefaultVersionType).Version(version).Refresh(DefaultRefresh).Id(id)
	if len(routing) > 0 {
		deleteService.Routing(routing)
	}
	_, err := deleteService.Do(ctx)
	return err
}

// 删除过程中如果命中的文档被修改会导致删除出现版本冲突，为了彻底删除文档，需要指定ProceedOnVersionConflict
func (c *Client) DeleteByQuery(ctx context.Context, indexName, id, routing string, query elastic.Query) error {
	deleteByQueryService := c.Client.DeleteByQuery(indexName).Query(query).ProceedOnVersionConflict().Refresh(DefaultRefresh)
	if len(routing) > 0 {
		deleteByQueryService.Routing(routing)
	}
	_, err := deleteByQueryService.Do(ctx)
	return err
}

func (c *Client) BulkDelete(indexName, id, routing string) {
	bulkDeleteRequest := elastic.NewBulkDeleteRequest().Index(indexName).Id(id)
	if len(routing) > 0 {
		bulkDeleteRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkDeleteRequest)
}

func (c *Client) BulkDeleteWithVersion(indexName, id, routing string, version int64) {
	bulkDeleteRequest := elastic.NewBulkDeleteRequest().Index(indexName).Id(id).VersionType(DefaultVersionType).Version(version)
	if len(routing) > 0 {
		bulkDeleteRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkDeleteRequest)
}

func (c *Client) Update(ctx context.Context, indexName, id, routing string, update map[string]interface{}) error {
	updateService := c.Client.Update().Index(indexName).Id(id).Refresh(DefaultRefresh)
	if len(routing) > 0 {
		updateService.Routing(routing)
	}
	_, err := updateService.Doc(update).Do(ctx)
	return err
}

func (c *Client) UpdateRefresh(ctx context.Context, indexName, id, routing string, update map[string]interface{}) error {
	updateService := c.Client.Update().Index(indexName).Id(id).Refresh(RefreshTrue)
	if len(routing) > 0 {
		updateService.Routing(routing)
	}
	_, err := updateService.Doc(update).Do(ctx)
	return err
}

func (c *Client) UpdateByQuery(ctx context.Context, indexName string, routings []string, query elastic.Query, script string, scriptParams map[string]interface{}) (*elastic.BulkIndexByScrollResponse, error) {
	updateByqueryService := c.Client.UpdateByQuery(indexName).Query(query).Script(elastic.NewScript(script).Params(scriptParams).Lang(DefaultScriptLang)).Refresh(DefaultRefresh).ProceedOnVersionConflict()
	if len(routings) > 0 {
		updateByqueryService.Routing(routings...)
	}
	return updateByqueryService.Do(ctx)
}

func (c *Client) BulkUpdate(indexName, id, routing string, update map[string]interface{}) {
	bulkUpdateRequest := elastic.NewBulkUpdateRequest().Index(indexName).Doc(update).Id(id)
	if len(routing) > 0 {
		bulkUpdateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkUpdateRequest)
}

func (c *Client) BulkUpdateDocs(ctx context.Context, index string, updates []*BulkUpdateDoc) (*elastic.BulkResponse, error) {
	bulkService := c.Client.Bulk().ErrorTrace(true).Refresh(DefaultRefresh)
	for _, update := range updates {
		doc := elastic.NewBulkUpdateRequest().Id(update.ID).Doc(update.update)
		if len(update.Routing) > 0 {
			doc.Routing(update.Routing)
		}
		bulkService.Add(doc)
	}
	return bulkService.Do(ctx)
}

// 文档结构必须完整
func (c *Client) UpsertWithVersion(ctx context.Context, indexName, id, routing string, doc interface{}, version int64) error {
	indexService := c.Client.Index().OpType("index").Index(indexName).Id(id).Refresh(DefaultRefresh).Version(version).VersionType(DefaultVersionType)
	if len(routing) > 0 {
		indexService.Routing(routing)
	}
	_, err := indexService.BodyJson(doc).Do(ctx)
	return err
}

// 不存在则插入
func (c *Client) Upsert(ctx context.Context, indexName, id, routing string, update map[string]interface{}, doc interface{}) error {
	updateService := c.Client.Update().Index(indexName).Id(id).Refresh(DefaultRefresh).DocAsUpsert(true)
	if len(routing) > 0 {
		updateService.Routing(routing)
	}
	_, err := updateService.Doc(update).Upsert(doc).Do(ctx)
	return err
}

// 异步upsert
func (c *Client) BulkUpsert(indexName, id, routing string, update map[string]interface{}, doc interface{}) {
	bulkUpdateRequest := elastic.NewBulkUpdateRequest().Index(indexName).Doc(update).Id(id).Upsert(doc).DocAsUpsert(true)
	if len(routing) > 0 {
		bulkUpdateRequest.Routing(routing)
	}
	c.BulkProcessor.Add(bulkUpdateRequest)
}

// UpsertBulk 批量upsert
func (c *Client) BulkUpsertDocs(ctx context.Context, index string, docs []*BulkUpsertDoc) (*elastic.BulkResponse, error) {
	bulkService := c.Client.Bulk().ErrorTrace(true).Refresh(DefaultRefresh)
	for _, doc := range docs {
		index := elastic.NewBulkUpdateRequest().Id(doc.ID).Doc(doc.update).Upsert(doc.Doc).DocAsUpsert(true)
		if len(doc.Routing) > 0 {
			index.Routing(doc.Routing)
		}
		bulkService.Add(index)
	}
	return bulkService.Do(ctx)
}
