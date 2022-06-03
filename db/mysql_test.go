package db

import "gitee.com/phper95/pkg/logger"

func initMysql() {
	err := InitMysqlClient(DefaultClient, "root", "", "127.0.0.1:3306", "shop")
	if err != nil {
		logger.Error("error")
	}
}
