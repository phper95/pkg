package main

import (
	"gitee.com/phper95/pkg/db"
	"go.uber.org/zap"
	"time"
)

func initMysql() {
	err := db.InitMysqlClient(db.DefaultClient, "root", "admin123", "localhost:3306", "shop")
	if err != nil {
		db.MysqltdLogger.Print("InitMysqlClient client error" + db.DefaultClient)
		return
	}
	db.MysqltdLogger.Print("connect mysql success ", zap.String("client", db.DefaultClient))
	err = db.InitMysqlClientWithOptions(db.TxClient, "root", "admin123", "localhost:3306", "shop", db.WithPrepareStmt(false))
	if err != nil {
		db.MysqltdLogger.Print("InitMysqlClient client error" + db.TxClient)
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

	//ormDB := db.GetMysqlClient(db.DefaultClient).DB
	ormDBTx := db.GetMysqlClient(db.TxClient).DB

	//查看连接配置
	//sqlDB, _ := ormDB.DB()
	//db.MysqltdLogger.Printf("Stats : %+v", sqlDB.Stats())

	//建表
	//if err := ormDB.AutoMigrate(&User{}); err != nil {
	//	db.MysqltdLogger.Print("AutoMigrate user error", err)
	//}

	//自定义表名的另一种方式
	//if err := ormDB.Table("user_table2").AutoMigrate(&User{}); err != nil {
	//	db.MysqltdLogger.Print("AutoMigrate user error", zap.Error(err))
	//}

	//name := ""
	//写入数据
	//user := User{
	//	Name: name,
	//	//Name:     &name,
	//	Age:      0,
	//	Birthday: nil,
	//	Email:    "111@qq.com",
	//}
	//if err := ormDB.Create(&user).Error; err != nil {
	//	db.MysqltdLogger.Print("insert error", zap.Any("user", user))
	//}

	// 指定字段创建
	//if err := ormDB.Select("email").Create(&user).Error; err != nil {
	//	db.MysqltdLogger.Print("insert error", zap.Any("user", user))
	//}

	// 批量创建
	//var users = []User{{Name: "user1", Email: "u1"}, {Name: "user2", Email: "u2"}, {Name: "user3", Email: "u3"}}
	//if err := ormDB.Create(&users).Error; err != nil {
	//	db.MysqltdLogger.Print("insert error", zap.Any("user", user))
	//}

	//查询时会忽略空值，o值，false和null值
	//users := make([]User, 0)
	//ormDB.Where(&user).Find(&users)
	//db.MysqltdLogger.Printf("%+v", users)

	//只获取指定字段
	//方式1
	//ormDB.Select("name", "email").Find(&users)
	//db.MysqltdLogger.Print("指定字段", users)
	//方式2 通过定义结构体来限制需要获取的字段
	//type APIUser struct {
	//	ID   uint
	//	Name string
	//}
	//ormDB.Model(&User{}).Limit(5).Find(&APIUser{}).Scan(&users)
	//
	//执行原生sql
	//查询
	//var userRes User
	//err := ormDB.Raw("select * from user where id = ?", 1).Scan(&userRes).Error
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println(userRes)
	//
	//err = ormDB.Exec("DROP TABLE user").Error
	//if err != nil {
	//	db.MysqltdLogger.Print("DROP TABLE error", err)
	//}

	//事务的使用
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
	//		db.MysqltdLogger.Print("Create user2 error", zap.Error(err))
	//	}
	//
	//	err = tx.Transaction(func(tx2 *gorm.DB) error {
	//		tx2.Create(&user3)
	//		return nil
	//	})
	//	if err != nil {
	//		db.MysqltdLogger.Print("Create user3 error", zap.Error(err))
	//	}
	//
	//	//返回值为nil时才会提交事务
	//	return nil
	//})
	//if err != nil {
	//	db.MysqltdLogger.Print("Transaction error", zap.Error(err))
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
	tx.Commit()            // 最终仅提交 user4

}
