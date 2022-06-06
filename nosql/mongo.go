package nosql

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/astaxie/beego/logs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

type mgClient struct {
	*mongo.Client
}

const (
	DefaultMongoClient = "default-mongo"
)

var (
	clients       = make(map[string]*mgClient, 0)
	printMongoURL string
)

func CheckMongoClient(clientName string) bool {
	if client, ok := clients[clientName]; ok {
		return client != nil
	} else {
		return false
	}
}

// InitMongo .
func InitMongo(clientName, mongodbURL string, mongoPoolLimit int) (err error) {
	var c *mongo.Client
	if strings.Index(mongodbURL, "@") != -1 {
		printMongoURL = mongodbURL[strings.Index(mongodbURL, "@"):len(mongodbURL)]
	} else {
		printMongoURL = mongodbURL
	}
	//链接mongo服务
	opt := options.Client().ApplyURI(mongodbURL)
	//opt.SetLocalThreshold(3 * time.Second)     //只使用与mongo操作耗时小于3秒的
	//opt.SetMaxConnIdleTime(30 * time.Minute)   //指定连接可以保持空闲的最大毫秒数
	opt.SetMaxPoolSize(uint64(mongoPoolLimit)) //使用最大的连接数
	//opt.SetMinPoolSize(uint64(mongoPoolLimit / 2)) //最小连接数
	if c, err = mongo.NewClient(opt); err != nil {
		logs.Error(err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		logs.Error(err)
		return err
	}
	//判断服务是否可用
	if err := c.Ping(getContext(), readpref.Primary()); err != nil {
		logs.Error(err)
		return err
	}
	mongoClient := mgClient{c}
	clients[clientName] = &mongoClient
	return nil

}

func MongoClientInstance(clientName string) *mgClient {
	if client, ok := clients[clientName]; ok {
		return client
	}
	panic("Call 'InitMongo' before!")
}

// Find .
func (client *mgClient) Find(db string, table string, query interface{}, projection interface{}, result interface{}) (bool, error) {
	//选择数据库和集合
	var (
		cursor *mongo.Cursor
		err    error
	)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), query, options.Find().SetSort(projection)); err != nil && err != mongo.ErrNoDocuments {
		logs.Error(err)
		return false, err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(err)
		return false, err
	}

	defer cursor.Close(context.Background())
	cursor.All(context.Background(), result)
	return true, nil
}

// SelectOne 查询一条数据
func (client *mgClient) SelectOne(db, table string, query interface{}, resultObj interface{}) error {
	result := client.Database(db).Collection(table).FindOne(getContext(), query)
	if result.Err() != nil && result.Err() != mongo.ErrNoDocuments {
		return result.Err()
	}
	if result.Decode(resultObj) != mongo.ErrNoDocuments {
		return result.Decode(resultObj)
	}
	return nil

}

// Select .
func (client *mgClient) Select(db string, table string, query interface{}, result interface{}) (bool, error) {
	//选择数据库和集合
	var cursor *mongo.Cursor
	collection := client.Database(db).Collection(table)
	var err error
	if cursor, err = collection.Find(getContext(), query); err != nil && err != mongo.ErrNoDocuments {
		logs.Error(err, db, table, printMongoURL)
		return false, err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(db, table, printMongoURL, err)
		return false, err
	}

	defer cursor.Close(context.Background())
	err = cursor.All(context.Background(), result)
	if err != nil {
		return false, err
	}
	return true, nil
}

// SelectByPage 分页取
func (client *mgClient) SelectByPageWithOpts(db string, table string, offset, limit int64, query interface{}, opts *options.FindOptions, result interface{}) (bool, error) {
	var (
		cursor *mongo.Cursor
		err    error
	)

	opts.SetLimit(limit).SetSkip(offset)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), query, opts); err != nil {
		logs.Error(err, db, table, printMongoURL)
		return false, err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(db, table, printMongoURL, err)
		return false, err
	}

	defer cursor.Close(context.Background())
	err = cursor.All(context.Background(), result)
	if err != nil {
		return false, err
	}
	return true, nil
}

// SelectByPage 分页取
func (client *mgClient) SelectByPage(db string, table string, offset, limit int64, query, result interface{}) (bool, error) {
	var (
		cursor *mongo.Cursor
		err    error
	)
	opts := &options.FindOptions{}
	opts.SetLimit(limit).SetSkip(offset)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), query, opts); err != nil {
		logs.Error(err, db, table, printMongoURL)
		return false, err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(db, table, printMongoURL, err)
		return false, err
	}

	defer cursor.Close(context.Background())
	err = cursor.All(context.Background(), result)
	if err != nil {
		return false, err
	}
	return true, nil
}

// 1 为升序排列，-1 降序排列
func (client *mgClient) SelectByPageWithSort(db string, table string, offset, limit int64, sortField string, sortValue int32, query, result interface{}) (bool, error) {
	var (
		cursor *mongo.Cursor
		err    error
	)
	opts := &options.FindOptions{}
	opts.SetLimit(limit).SetSkip(offset).SetSort(bsonx.Doc{{sortField, bsonx.Int32(sortValue)}})
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), query, opts); err != nil {
		logs.Error(err, db, table, printMongoURL)
		return false, err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(db, table, printMongoURL, err)
		return false, err
	}

	defer cursor.Close(context.Background())
	err = cursor.All(context.Background(), result)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (client *mgClient) GetAllByCursor(db string, table string, limit, offset int64, query, row interface{}, callback func(res interface{}) (bool, error)) error {
	var (
		cursor *mongo.Cursor
		err    error
	)
	opts := &options.FindOptions{}
	opts.SetLimit(limit).SetSkip(offset)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), query); err != nil {
		logs.Error(err, db, table, printMongoURL)
		return err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(db, table, printMongoURL, err)
		return err
	}

	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		err = cursor.Decode(row)
		if err != nil {
			logs.Error(db, table, printMongoURL, err)
		} else {
			_, _ = callback(row)
		}
	}
	return nil
}

// SelectByPage 分页取
func (client *mgClient) SelectByPageUseCursor(db string, table string, offset, limit int64, query, row interface{}, callback func(row interface{})) error {
	var (
		cursor *mongo.Cursor
		err    error
	)
	opts := &options.FindOptions{}
	opts.SetLimit(limit).SetSkip(offset)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), query, opts); err != nil {
		logs.Error(err, db, table, printMongoURL)
		return err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(db, table, printMongoURL, err)
		return err
	}

	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		err = cursor.Decode(row)
		if err != nil {
			logs.Error(db, table, printMongoURL, err)
		} else {
			callback(row)
		}
	}
	return nil
}

func (client *mgClient) SelectUseCursor(db string, table string, batchSize int32, query, row interface{}, callback func(row interface{})) error {
	var (
		cursor *mongo.Cursor
		err    error
	)
	opts := &options.FindOptions{}
	opts.SetBatchSize(batchSize)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), query, opts); err != nil {
		logs.Error(err, db, table, printMongoURL)
		return err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(db, table, printMongoURL, err)
		return err
	}

	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		err = cursor.Decode(row)
		if err != nil {
			logs.Error(db, table, printMongoURL, err)
		} else {
			callback(row)
		}
	}
	return err
}
func (client *mgClient) SelectUseCursorWithCallbackParams(db string, table string, batchSize int32, query, row interface{}, params interface{}, callback func(row interface{}, params interface{})) error {
	var (
		cursor *mongo.Cursor
		err    error
	)
	opts := &options.FindOptions{}
	opts.SetBatchSize(batchSize)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), query, opts); err != nil {
		logs.Error(err, db, table, printMongoURL)
		return err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(db, table, printMongoURL, err)
		return err
	}

	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		err = cursor.Decode(row)
		if err != nil {
			logs.Error(db, table, printMongoURL, err)
		} else {
			callback(row, params)
		}
	}
	return err
}

func (client *mgClient) SelectUseCursorWithFindOptions(db string, table string, batchSize int32, query, row interface{}, opts *options.FindOptions, callback func(row interface{})) error {
	var (
		cursor *mongo.Cursor
		err    error
	)
	opts.SetBatchSize(batchSize)
	collection := client.Database(db).Collection(table)
	if cursor, err = collection.Find(getContext(), query, opts); err != nil {
		logs.Error(err, db, table, printMongoURL)
		return err
	}
	if err = cursor.Err(); err != nil {
		logs.Error(db, table, printMongoURL, err)
		return err
	}

	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		err = cursor.Decode(row)
		if err != nil {
			logs.Error(db, table, printMongoURL, err)
		} else {
			callback(row)
		}
	}
	return err
}

func (client *mgClient) Aggregate(db string, table string, queries []bson.D, row interface{}, callback func(row interface{})) error {
	pipeline := mongo.Pipeline{}
	for _, q := range queries {
		pipeline = append(pipeline, q)
	}
	cursor, err := client.Database(db).Collection(table).Aggregate(context.Background(), pipeline, options.Aggregate())
	if cursor != nil {
		defer cursor.Close(context.Background())
		for cursor.Next(context.Background()) {
			err = cursor.Decode(row)
			if err != nil {
				logs.Error(db, table, printMongoURL, "Aggregate", err)
			} else {
				callback(row)
			}
		}
	}
	return err

}

// Insert .
func (client *mgClient) Insert(db string, table string, docs ...interface{}) (bool, error) {
	var err error
	collection := client.Database(db).Collection(table)
	if _, err = collection.InsertMany(getContext(), docs); err != nil {
		logs.Debug(db, table, printMongoURL, err)
		return false, err
	}
	//fmt.Printf("InsertMany插入的消息ID:%v\n", insertManyRes.InsertedIDs)
	return true, nil
}

// Upsert doc是bson格式
func (client *mgClient) Upsert(db string, table string, query interface{}, doc interface{}) (bool, error) {
	collection := client.Database(db).Collection(table)
	//设置Upset设置项
	opts := options.FindOneAndUpdate().SetUpsert(true)
	res := collection.FindOneAndUpdate(getContext(), query, doc, opts)
	err := res.Err()
	//插入
	if err != nil && err == mongo.ErrNoDocuments {
		return true, nil
	} else if err != nil {
		logs.Debug(db, table, printMongoURL, err)
		return false, err
	}

	return true, nil
}

func (client *mgClient) Replace(db string, table string, query interface{}, doc interface{}) (bool, error) {
	doc, err := BsontoDoc(doc)
	if err != nil {
		logs.Error("BsontoDoc error", doc)
		return false, err
	}
	collection := client.Database(db).Collection(table)

	//设置Replace设置项
	opts := options.Replace().SetUpsert(true)
	_, err = collection.ReplaceOne(getContext(), query, doc, opts)
	if err != nil && err == mongo.ErrNoDocuments {
		logs.Error(db, table, printMongoURL, err)
		return false, err
	}

	return true, nil
}

// Update .
func (client *mgClient) UpdateOne(db string, table string, query interface{}, doc interface{}) (bool, error) {
	doc, err := BsontoDoc(doc)
	if err != nil {
		logs.Error("BsontoDoc error", doc)
		return false, err
	}
	collection := client.Database(db).Collection(table)
	_, err = collection.UpdateOne(getContext(), query, bson.M{"$set": doc}, nil)
	if err != nil {
		logs.Error("UpdateUseStruct error ", db, table, printMongoURL, err)
		return false, err
	}
	return true, nil
}

func (client *mgClient) Update(db string, table string, query interface{}, doc interface{}) (bool, error) {
	collection := client.Database(db).Collection(table)
	_, err := collection.UpdateOne(getContext(), query, doc, nil)
	if err != nil {
		logs.Error(db, table, printMongoURL, err)
		return false, err
	}
	return true, nil
}

// DeleteOne .
func (client *mgClient) DeleteOne(db string, table string, filter map[string]interface{}) (bool, error) {
	collection := client.Database(db).Collection(table)
	//还原int类型
	replaceFloatsWithInts(filter)
	d, err := collection.DeleteOne(getContext(), filter, nil)
	if err != nil {
		if err == mongo.ErrNoDocuments || err == mongo.ErrNilDocument {
			return true, nil
		}
		logs.Error("DeleteOne", db, table, printMongoURL, err, d)
		return false, err
	}
	return true, nil
}

// DeleteOne .
func (client *mgClient) DeleteMany(db string, table string, filter map[string]interface{}) (bool, error) {
	collection := client.Database(db).Collection(table)
	replaceFloatsWithInts(filter)
	d, err := collection.DeleteMany(getContext(), filter, nil)
	logs.Debug(d, filter, err)
	if err != nil {
		if err == mongo.ErrNoDocuments || err == mongo.ErrNilDocument {
			return true, nil
		}
		logs.Error("DeleteMany", db, table, printMongoURL, err, d)
		return false, err
	}
	return true, nil
}

// Count .
func (client *mgClient) Count(db, table string, query interface{}, defaultVal int) (int, error) {
	collection := client.Database(db).Collection(table)
	count, err := collection.CountDocuments(getContext(), query, nil)
	if err != nil {
		logs.Error("Count", db, table, printMongoURL, count, err)
		return defaultVal, err
	}
	return int(count), err
}

//通过metadata获取整个集合中总记录数
func (client *mgClient) GetEstimatedDocumentCount(db, table string) (int64, error) {
	collection := client.Database(db).Collection(table)
	count, err := collection.EstimatedDocumentCount(getContext(), nil)
	if err != nil {
		logs.Error("GetEstimatedDocumentCount", db, table, printMongoURL, count, err)
		return count, err
	}
	return count, err
}

// Distinct .
func (client *mgClient) Distinct(db, table string, query interface{}, distinctField string) (result []interface{}, err error) {
	collection := client.Database(db).Collection(table)
	return collection.Distinct(getContext(), distinctField, query, nil)
}

// CreateIndex .
func (client *mgClient) CreateIndex(db, table, key string, uniqueKey bool) (bool, error) {
	collection := client.Database(db).Collection(table)
	_, err := collection.Indexes().CreateOne(getContext(),
		mongo.IndexModel{
			Keys:    bsonx.Doc{{key, bsonx.Int32(-1)}},
			Options: options.Index().SetUnique(uniqueKey),
		})
	if err != nil {
		return false, err
	}
	return true, nil
}

// CreateComplexIndex .
func (client *mgClient) CreateComplexIndex(db, table string, keys []string, uniqueKey bool) (bool, error) {
	collection := client.Database(db).Collection(table)
	doc := bsonx.Doc{}
	for _, key := range keys {
		doc = doc.Append(key, bsonx.Int32(-1))
	}
	_, err := collection.Indexes().CreateOne(getContext(),
		mongo.IndexModel{
			Keys:    doc,
			Options: options.Index().SetUnique(uniqueKey),
		})
	if err != nil {
		return false, err
	}
	return true, nil
}

// CreateIndexWithBackground .
func (client *mgClient) CreateIndexWithBackground(db, table, key string, uniqueKey bool) (bool, error) {
	collection := client.Database(db).Collection(table)
	opts := options.Index()
	opts.SetBackground(true)
	_, err := collection.Indexes().CreateOne(context.Background(),
		mongo.IndexModel{
			Keys:    bsonx.Doc{{key, bsonx.Int32(-1)}},
			Options: options.Index().SetUnique(uniqueKey).SetBackground(true),
		})
	if err != nil {
		return false, err
	}
	return true, nil
}

// CreateIndex .
func (client *mgClient) CreateIndices(db, table string, indexSettings map[string]bool) (bool, error) {
	collection := client.Database(db).Collection(table)
	indexModels := make([]mongo.IndexModel, 0)
	for key, uniqueKey := range indexSettings {
		indexModel := mongo.IndexModel{
			Keys:    bsonx.Doc{{key, bsonx.Int32(-1)}},
			Options: options.Index().SetUnique(uniqueKey),
		}
		indexModels = append(indexModels, indexModel)
	}

	_, err := collection.Indexes().CreateMany(getContext(), indexModels)

	if err != nil {
		return false, err
	}
	return true, nil
}

// DeleteIndex .
func (client *mgClient) DeleteIndex(db, table string) (bool, error) {
	err := client.Database(db).Collection(table).Drop(getContext())
	if err != nil {
		return false, err
	}
	return true, nil
}

// RemoveAll .
func (client *mgClient) RemoveAll(db string, table string, query interface{}) (bool, error) {
	collection := client.Database(db).Collection(table)
	if _, err := collection.DeleteMany(getContext(), query); err != nil && err != mongo.ErrNoDocuments {
		logs.Error(err)
		return false, err
	}
	return true, nil
}

func getContext() (ctx context.Context) {
	ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
	return
}

func (client *mgClient) RenameTable(db, table, newTable string) (bool, error) {
	cmd := bson.D{
		{"renameCollection", strings.Join([]string{db, table}, ".")},
		{"to", strings.Join([]string{db, newTable}, ".")},
	}
	//注意:只有admin库才有执行renameCollection的权限
	b, err := client.Database("admin").RunCommand(getContext(), cmd).DecodeBytes()
	if err != nil {
		logs.Error(err, db, table, printMongoURL)
	}
	if b != nil && b.Index(0).Value().Double() == 1 {
		return true, nil
	} else {
		if b != nil && b.Index(1).Validate() == nil {
			return false, errors.New(b.Index(1).String())
		}
		logs.Error(err, b, db, table, printMongoURL)
		if b != nil {
			return false, errors.New(b.String())
		}
		return false, errors.New("rename failed")
	}
}

// DuplicateTable 用于备份
func (client *mgClient) DuplicateTable(db, table, newTable string) (bool, error) {
	_, err := client.Database(db).Collection(table, options.Collection().SetReadPreference(readpref.Primary()).SetReadConcern(readconcern.Local())).Aggregate(getContext(), []interface{}{bson.M{"$out": newTable}})
	logs.Debug(err)
	if err != nil {
		return false, err
	}
	return true, nil

}

func BsontoDoc(v interface{}) (doc *bson.Raw, err error) {
	data, err := bson.Marshal(v)
	if err != nil {
		return
	}
	err = bson.Unmarshal(data, &doc)
	return
}

//其他类型转map过程中会把本身的int类型转化成了float64，这里做一次还原
func replaceFloatsWithInts(m map[string]interface{}) {
	for key, val := range m {
		if f, ok := val.(float64); ok && f == math.Floor(f) {
			m[key] = int32(f)
			continue
		}

		if innerM, ok := val.(map[string]interface{}); ok {
			replaceFloatsWithInts(innerM)
			m[key] = innerM
		}
	}
}

func (client *mgClient) Close() {
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := client.Disconnect(ctx)
	if err != nil {
		logs.Error("mongo close err:", err)
	}
	logs.Warn("closed :mongoDb")
}
