package pay

import (
	"fmt"
	"gitee.com/phper95/pkg/pay/client"
	"gitee.com/phper95/pkg/pay/common"
	"gitee.com/phper95/pkg/pay/constant"
	"net/http"
	"testing"
)

// 测试
func TestPay(t *testing.T) {
	initClient()
	initHandle()
	charge := new(common.Charge)
	charge.PayMethod = constant.WECHAT
	charge.MoneyFee = 1
	charge.Describe = "测试订单"
	charge.TradeNum = "8888888888"

	fdata, err := Pay(charge)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(fdata)
}

// 初始化客户端
func initClient() {
	client.InitAliAppClient(&client.AliAppClient{
		PartnerID:  "xxx",
		SellerID:   "xxxx",
		AppID:      "xxx",
		PrivateKey: nil,
		PublicKey:  nil,
	})
}

// 初始化处理
func initHandle() {
	http.HandleFunc("callback/aliappcallback", func(w http.ResponseWriter, r *http.Request) {
		aliResult, err := AliAppCallback(w, r)
		if err != nil {
			fmt.Println(err)
			//log.xxx 日志处理
			return
		}
		selfHandler(aliResult)
	})
}

func selfHandler(i interface{}) {
}
