package common

// 支付客户端接口
type PayClient interface {
	Pay(charge *Charge) (string, error)
	//检查签名
	//CheckSign(data []byte, sign []byte) error
}

//支付参数
type Charge struct {
	TradeNum    string `json:"tradeNum,omitempty"`
	Origin      string `json:"origin,omitempty"`
	UserID      string `json:"userId,omitempty"`
	PayMethod   int64  `json:"payMethod,omitempty"`
	MoneyFee    int64  `json:"MoneyFee,omitempty"`
	CallbackURL string `json:"callbackURL,omitempty"`
	ReturnURL   string `json:"returnURL,omitempty"`
	ShowURL     string `json:"showURL,omitempty"`
	Describe    string `json:"describe,omitempty"`
	OpenID      string `json:"openid,omitempty"`
}

//支付返回
type PayCallback struct {
	Origin      string `json:"origin"`
	TradeNum    string `json:"trade_num"`
	OrderNum    string `json:"order_num"`
	CallBackURL string `json:"callback_url"`
	Status      int64  `json:"status"`
}

// 回调业务代码时的参数
type CallbackReturn struct {
	IsSucceed     bool   `json:"isSucceed"`
	OrderNum      string `json:"orderNum"`
	TradeNum      string `json:"tradeNum"`
	UserID        string `json:"userID"`
	MoneyFee      int64  `json:"moneyFee"`
	Sign          string `json:"sign"`
	ThirdDiscount int64  `json:"thirdDiscount"`
}

// 支付结果
type BaseResult struct {
	IsSucceed     bool   // 是否交易成功
	TradeNum      string // 交易流水号
	MoneyFee      int64  // 支付金额
	TradeTime     string // 交易时间
	ContractNum   string // 交易单号
	UserInfo      string // 支付账号信息(有可能有，有可能没有)
	ThirdDiscount int64  // 第三方优惠
}
