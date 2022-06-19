package main

import (
	"gitee.com/phper95/nosql"
)

type User struct {
	ID   int64  `bson:"id"`
	Name string `bson:"name"`
}

type userWithID struct {
	ID   int64  `bson:"_id"`
	Name string `bson:"name"`
}

//type userWithPrimitiveID struct {
//	ID   primitive.ObjectID `bson:"_id"`
//	Name string             `bson:"name"`
//}

const (
	DBName    = "imooc"
	TableName = "test"
)

func main() {
	err := nosql.InitMongoClient(nosql.DefaultMongoClient, "admin", "admin123", []string{"127.0.0.1:27017"}, 100)
	if err != nil {
		nosql.MongoStdLogger.Print(err)
		panic("InitMongoClient failed")
	}

	mongoClient := nosql.GetMongoClient(nosql.DefaultMongoClient)
	//1.插入
	//user := User{
	//	ID:   1,
	//	Name: "test1",
	//}
	//err = mongoClient.InsertMany(DBName, TableName, user)
	//if err != nil {
	//	nosql.MongoStdLogger.Print(err)
	//}
	//user2 := map[string]interface{}{
	//	"id":   2,
	//	"name": "test2",
	//}
	//err = mongoClient.InsertMany(DBName, TableName, user2)
	//if err != nil {
	//	nosql.MongoStdLogger.Print(err)
	//}

	//2. 指定主键
	//map中指定主键
	//user3 := map[string]interface{}{
	//	"_id":  3,
	//	"name": "test3",
	//}
	//err = mongoClient.InsertMany(DBName, TableName, user3)
	//if err != nil {
	//	nosql.MongoStdLogger.Print("user3 error", err)
	//}

	//user4 := userWithID{
	//	ID:   4,
	//	Name: "test4",
	//}
	//err = mongoClient.InsertMany(DBName, TableName, user4)
	//if err != nil {
	//	nosql.MongoStdLogger.Print(err)
	//}
	//3.写入的时候数据格式不一样

	//user5 := map[string]interface{}{
	//	"id":   1,
	//	"name": "test1",
	//}
	//err = mongoClient.InsertMany(DBName, "test2", user5)
	//if err != nil {
	//	nosql.MongoStdLogger.Print(err)
	//}
	//
	//user6 := map[string]interface{}{
	//	"id":   "2",
	//	"name": "test2",
	//}
	//err = mongoClient.InsertMany(DBName, "test2", user6)
	//if err != nil {
	//	nosql.MongoStdLogger.Print("user6 insert error : ", err)
	//}

	//user7 := map[string]string{
	//	"id":   "3",
	//	"name": "test3",
	//}
	//err = mongoClient.InsertMany(DBName, "test2", user7)
	//if err != nil {
	//	nosql.MongoStdLogger.Print("user7 insert error : ", err)
	//}

	//5.混用自定义id和自动生成的id 在查询时出现的问题
	//res := make([]userWithID, 0)
	//mongoClient.Find(DBName, TableName, bson.D{}, &res)
	//nosql.MongoStdLogger.Print("res :", res)

	//6.更新
	//update := map[string]interface{}{"name": "test33"}
	//err = mongoClient.UpdateOne(DBName, TableName, bson.D{{"_id", 3}}, update)
	//if err != nil {
	//	nosql.MongoStdLogger.Print("user7 insert error : ", err)
	//}

	//7.upsert
	//update := map[string]interface{}{"name": "test333"}
	//err = mongoClient.Upsert(DBName, TableName, bson.D{{"_id", 4}}, update)
	//if err != nil {
	//	nosql.MongoStdLogger.Print("user7 insert error : ", err)
	//}
	//8.replace
	//err = mongoClient.ReplaceOne(DBName, TableName, bson.D{{"_id", 8}}, userWithID{
	//	ID:   8,
	//	Name: "test867",
	//})
	//if err != nil {
	//	nosql.MongoStdLogger.Print("user8 insert error : ", err)
	//}

	//条件查询
	//res := make([]userWithID, 0)
	//mongoClient.Find(DBName, TableName, bson.D{{"_id", bson.M{"$gt": 0}}}, &res)
	//nosql.MongoStdLogger.Print("Find res :", res)

	//9.游标查询
	//err = mongoClient.FindUseCursor(DBName, TableName, 1, bson.D{}, &userWithID{}, callback)
	//if err != nil {
	//	nosql.MongoStdLogger.Print("FindUseCursor error : ", err)
	//}
	// 10. count
	//count, err := mongoClient.EstimatedDocumentCount(DBName, TableName)
	//nosql.MongoStdLogger.Print("EstimatedDocumentCount : ", " count : ", count, " error : ", err)
	//11.删除

	//$in 中不存在的id不抛异常
	//err = mongoClient.DeleteMany(DBName, TableName, bson.D{{"_id", bson.D{{"$in", []int64{4, 7}}}}})
	//if err != nil {
	//	nosql.MongoStdLogger.Print("DeleteOne : ", " error : ", err)
	//}

	//12.创建索引
	err = mongoClient.CreateIndex(DBName, TableName, "name", false)
	if err != nil {
		nosql.MongoStdLogger.Print("CreateIndex : ", " error : ", err)
	}

}

//游标会超时，所以在回调函数内部，一般不宜耦合过多操作
//可以将回调中查询到的数放到本地数组或者map中
func callback(res interface{}, err error) {
	nosql.MongoStdLogger.Print(res, err)
}
