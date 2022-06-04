package main

import (
	"fmt"
	"gitee.com/phper95/pkg/db"
	"go.uber.org/zap"
	"time"
)

var logger *zap.Logger

func init() {
	logger, _ = zap.NewDevelopment()
}

func initMysql() {
	err := db.InitMysqlClient(db.DefaultClient, "root", "admin123", "localhost:3306", "shop")
	if err != nil {
		logger.Error("InitMysqlClient client error" + db.DefaultClient)
		return
	}
	logger.Debug("connect mysql success ", zap.String("client", db.DefaultClient))
	err = db.InitMysqlClientWithOptions(db.TxClient, "root", "admin123", "localhost:3306", "shop", db.WithPrepareStmt(false))
	if err != nil {
		logger.Error("InitMysqlClient client error" + db.TxClient)
		return
	}

}

//type User struct {
//	gorm.Model
//	Name     string
//	Age      int `gorm:"type:tinyint(3);unsigned"`
//	Birthday *time.Time
//	Email    string `gorm:"type:varchar(100);unique"`
//}

//字段属性设置
type User struct {
	ID uint //bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY
	//ID       uint64
	//UserID uint `gorm:"primarykey;AUTO_INCREMENT"` //自定义主键
	//Name   string
	Name string `gorm:"type:varchar(255);NOT NULL;DEFAULT:''"`
	//Name     *string
	Age      int `gorm:"type:tinyint(3);unsigned"`
	Birthday *time.Time
	Email    string `gorm:"type:varchar(100);unique"`
}

//type Tabler interface {
//	TableName() string
//}
//
//// TableName 会将 User 的表名重写为 `user_table1`
//func (User) TableName() string {
//	return "user_table1"
//}
func main() {
	initMysql()

	ormDB := db.GetMysqlClient(db.DefaultClient).DB
	ormDBTx := db.GetMysqlClient(db.TxClient).DB

	//建表
	if err := ormDB.AutoMigrate(&User{}); err != nil {
		logger.Error("AutoMigrate user error", zap.Error(err))
	}

	//自定义表名的另一种方式
	//if err := ormDB.Table("user_table2").AutoMigrate(&User{}); err != nil {
	//	logger.Error("AutoMigrate user error", zap.Error(err))
	//}

	name := ""
	//写入数据
	user := User{
		Name: name,
		//Name:     &name,
		Age:      0,
		Birthday: nil,
		Email:    "111@qq.com",
	}
	//if err := ormDB.Create(&user).Error; err != nil {
	//	logger.Error("insert error", zap.Any("user", user))
	//}

	// 指定字段创建
	//if err := ormDB.Select("email").Create(&user).Error; err != nil {
	//	logger.Error("insert error", zap.Any("user", user))
	//}

	// 批量创建
	//var users = []User{{Name: "user1", Email: "u1"}, {Name: "user2", Email: "u2"}, {Name: "user3", Email: "u3"}}
	//if err := ormDB.Create(&users).Error; err != nil {
	//	logger.Error("insert error", zap.Any("user", user))
	//}

	//查询时会忽略空值，o值，false和null值
	users := make([]*User, 0)
	ormDB.Where(&user).Find(&users)
	fmt.Printf("%+v", users)

	//user1 := User{
	//	Name:     "user1",
	//	Age:      0,
	//	Birthday: nil,
	//	Email:    "user1@qq.com",
	//}
	//
	//user2 := User{
	//	Name:     "user2",
	//	Age:      0,
	//	Birthday: nil,
	//	Email:    "user2@qq.com",
	//}
	//
	//user3 := User{
	//	Name:     "user3",
	//	Age:      0,
	//	Birthday: nil,
	//	Email:    "user3@qq.com",
	//}
	//嵌套事务
	//err := ormDBTx.Transaction(func(tx *gorm.DB) error {
	//	//注意，内部需要使用tx，而不是ormDB
	//	tx.Create(&user1)
	//
	//	err := tx.Transaction(func(tx2 *gorm.DB) error {
	//		tx2.Create(&user2)
	//		return errors.New("rollback user2") // 回滚 user2
	//	})
	//	if err != nil {
	//		logger.Error("Create user2 error", zap.Error(err))
	//	}
	//
	//	err = tx.Transaction(func(tx2 *gorm.DB) error {
	//		tx2.Create(&user3)
	//		return nil
	//	})
	//	if err != nil {
	//		logger.Error("Create user3 error", zap.Error(err))
	//	}
	//
	//	//返回值为nil时才会提交事务
	//	return nil
	//})
	//if err != nil {
	//	logger.Error("Transaction error", zap.Error(err))
	//}

	user4 := User{
		Name:     "user4",
		Age:      0,
		Birthday: nil,
		Email:    "user4@qq.com",
	}

	user5 := User{
		Name:     "user5",
		Age:      0,
		Birthday: nil,
		Email:    "user5@qq.com",
	}
	//检查点和回滚点
	tx := ormDBTx.Begin()
	tx.Create(&user4)

	tx.SavePoint("step1")
	tx.Create(&user5)
	tx.RollbackTo("step1") // 回滚 user2

	tx.Commit() // 最终仅提交 user4
}
