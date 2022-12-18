package main

import (
	"fmt"
	"gitee.com/phper95/pkg/pay"
	"gitee.com/phper95/pkg/pay/client"
	"gitee.com/phper95/pkg/pay/common"
	"gitee.com/phper95/pkg/pay/constant"
	"net/http"
)

func main() {
	//设置支付宝账号信息
	initClient()
	//设置回调函数
	initHandle()

	//支付
	charge := new(common.Charge)
	charge.PayMethod = constant.ALI_APP                             //支付方式
	charge.MoneyFee = 1                                             // 支付钱单位分
	charge.Describe = "测试订单"                                        //支付描述
	charge.TradeNum = "88888888"                                    //交易号
	charge.CallbackURL = "http://127.0.0.1/callback/aliappcallback" //回调地址必须跟下面一样
	//导入包
	fdata, err := pay.Pay(charge)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(fdata)
}

func initClient() {
	client.InitAliAppClient(&client.AliAppClient{
		PartnerID:  "xxx",
		SellerID:   "xxxx",
		AppID:      "xxx",
		PrivateKey: nil,
		PublicKey:  nil,
	})
}

func initHandle() {
	http.HandleFunc("callback/aliappcallback", func(w http.ResponseWriter, r *http.Request) {
		//返回支付结果
		aliResult, err := pay.AliAppCallback(w, r)
		if err != nil {
			fmt.Println(err)
			//log.xxx
			return
		}
		//接下来处理自己的逻辑
		fmt.Println(aliResult)
	})
}
