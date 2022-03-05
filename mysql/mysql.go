package mysql

import (
	"fmt"
	"gitee.com/phper95/pkg/errors"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
)

// Predicate is a string that acts as a condition in the where clause
type Predicate string
type option struct {
	MaxOpenConn     int
	MaxIdleConn     int
	ConnMaxLifeTime time.Duration
}
type Option func(*option)

var (
	EqualPredicate              = Predicate("=")
	NotEqualPredicate           = Predicate("<>")
	GreaterThanPredicate        = Predicate(">")
	GreaterThanOrEqualPredicate = Predicate(">=")
	SmallerThanPredicate        = Predicate("<")
	SmallerThanOrEqualPredicate = Predicate("<=")
	LikePredicate               = Predicate("LIKE")
)
var mysqlClients = make(map[string]*gorm.DB)

func (o *option) reset() {
	o.MaxOpenConn = 0
	o.MaxIdleConn = 0
	o.ConnMaxLifeTime = 0
}
func WithMaxOpenConn(maxOpenConn int) Option {
	return func(opt *option) {
		opt.MaxOpenConn = maxOpenConn
	}
}

func WithMaxIdleConn(maxIdleConn int) Option {
	return func(opt *option) {
		opt.MaxIdleConn = maxIdleConn
	}
}

func WithConnMaxLifeTime(connMaxLifeTime time.Duration) Option {
	return func(opt *option) {
		opt.ConnMaxLifeTime = connMaxLifeTime
	}
}

func InitMysqlClient(clientName, username, password, addr, dbName string, options ...Option) error {
	if len(clientName) == 0 {
		return errors.New("client name is empty")
	}
	if len(username) == 0 {
		return errors.New("username is empty")
	}
	opt := &option{}
	for _, f := range options {
		f(opt)
	}

	db, err := dbConnect(username, password, addr, dbName, opt)
	if err != nil {
		return err
	}
	mysqlClients[clientName] = db
	return nil
}
func GetMysqlClient(clientName string) *gorm.DB {
	if client, ok := mysqlClients[clientName]; ok {
		return client
	}
	return nil
}

func CloseMysqlClient(clientName string) error {
	sqlDB, err := GetMysqlClient(clientName).DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func dbConnect(user, pass, addr, dbName string, option *option) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=%t&loc=%s",
		user,
		pass,
		addr,
		dbName,
		true,
		"Local")

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
		//Logger: logger.Default.LogMode(logger.Info), // 日志配置
	})

	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("[db connection failed] Database name: %s", dbName))
	}

	db.Set("gorm:table_options", "CHARSET=utf8mb4")

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// 设置连接池 用于设置最大打开的连接数，默认值为0表示不限制.设置最大的连接数，可以避免并发太高导致连接mysql出现too many connections的错误。
	if option.MaxOpenConn > 0 {
		sqlDB.SetMaxOpenConns(option.MaxOpenConn)
	}

	// 设置最大连接数 用于设置闲置的连接数.设置闲置的连接数则当开启的一个连接使用完成后可以放在池里等候下一次使用。
	if option.MaxIdleConn > 0 {
		sqlDB.SetMaxIdleConns(option.MaxIdleConn)
	}

	// 设置最大连接超时
	if option.ConnMaxLifeTime > 0 {
		sqlDB.SetConnMaxLifetime(time.Minute * option.ConnMaxLifeTime)
	}

	// 使用插件
	db.Use(&TracePlugin{})

	return db, nil
}
