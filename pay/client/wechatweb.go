package client

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"gitee.com/phper95/pkg/pay/common"
	"gitee.com/phper95/pkg/pay/util"
	"sort"
	"strings"
	"time"
)

var defaultWechatWebClient *WechatWebClient

func DefaultWechatWebClient() *WechatWebClient {
	return defaultWechatWebClient
}

// WechatWebClient 微信公众号支付
type WechatWebClient struct {
	AppID       string // 公众账号ID
	MchID       string // 商户号ID
	CallbackURL string // 回调地址
	Key         string // 密钥
	PayURL      string // 支付地址
	QueryURL    string // 查询地址
}

// Pay 支付
func (wechat *WechatWebClient) Pay(charge *common.Charge) (string, error) {
	var m = make(map[string]string)
	m["appid"] = wechat.AppID
	m["mch_id"] = wechat.MchID
	m["nonce_str"] = util.RandomStr()
	m["body"] = charge.Describe
	m["out_trade_no"] = charge.TradeNum
	m["total_fee"] = fmt.Sprintf("%d", charge.MoneyFee)
	m["spbill_create_ip"] = util.LocalIP()
	m["notify_url"] = wechat.CallbackURL
	m["trade_type"] = "JSAPI"
	m["openid"] = charge.OpenID

	sign, err := wechat.GenSign(m)
	if err != nil {
		return "", err
	}
	m["sign"] = sign
	// 转出xml结构
	buf := bytes.NewBufferString("")
	for k, v := range m {
		buf.WriteString(fmt.Sprintf("<%s><![CDATA[%s]]></%s>", k, v, k))
	}
	xmlStr := fmt.Sprintf("<xml>%s</xml>", buf.String())

	re, err := HTTPSC.PostData(wechat.PayURL, "text/xml:charset=UTF-8", xmlStr)
	if err != nil {
		return "", err
	}
	var xmlRe common.WeChatReResult
	err = xml.Unmarshal(re, &xmlRe)
	if err != nil {
		return "", err
	}

	if xmlRe.ReturnCode != "SUCCESS" {
		// 通信失败
		return "", errors.New(xmlRe.ReturnMsg)
	}

	if xmlRe.ResultCode != "SUCCESS" {
		// 支付失败
		return "", errors.New(xmlRe.ErrCodeDes)
	}

	var c = make(map[string]string)
	c["appId"] = wechat.AppID
	c["timeStamp"] = fmt.Sprintf("%d", time.Now().Unix())
	c["nonceStr"] = util.RandomStr()
	c["package"] = fmt.Sprintf("prepay_id=%s", xmlRe.PrepayID)
	c["signType"] = "MD5"

	sign2, err := wechat.GenSign(c)
	if err != nil {
		return "", err
	}
	c["paySign"] = sign2

	jsonC, err := json.Marshal(c)
	if err != nil {
		return "", err
	}

	return string(jsonC), nil
}

// GenSign 产生签名
func (wechat *WechatWebClient) GenSign(m map[string]string) (string, error) {
	delete(m, "sign")
	delete(m, "Key")
	var signData []string
	for k, v := range m {
		if v != "" {
			signData = append(signData, fmt.Sprintf("%s=%s", k, v))
		}
	}

	sort.Strings(signData)
	signStr := strings.Join(signData, "&")
	signStr = signStr + "&Key=" + wechat.Key
	c := md5.New()
	_, err := c.Write([]byte(signStr))
	if err != nil {
		return "", err
	}
	signByte := c.Sum(nil)
	if err != nil {
		return "", err
	}
	return strings.ToUpper(fmt.Sprintf("%x", signByte)), nil
}

// CheckSign 检查签名
func (wechat *WechatWebClient) CheckSign(data string, sign string) error {
	signData := data + "&Key=" + wechat.Key
	c := md5.New()
	_, err := c.Write([]byte(signData))
	if err != nil {
		return err
	}
	signOut := fmt.Sprintf("%x", c.Sum(nil))
	if strings.ToUpper(sign) == strings.ToUpper(signOut) {
		return nil
	}
	return errors.New("签名交易错误")
}

// QueryOrder 查询订单
func (wechat *WechatWebClient) QueryOrder(tradeNum string) (*common.WeChatQueryResult, error) {
	var m = make(map[string]string)
	m["appid"] = wechat.AppID
	m["mch_id"] = wechat.MchID
	m["out_trade_no"] = tradeNum
	m["nonce_str"] = util.RandomStr()

	sign, err := wechat.GenSign(m)
	if err != nil {
		return nil, err
	}
	m["sign"] = sign

	buf := bytes.NewBufferString("")
	for k, v := range m {
		buf.WriteString(fmt.Sprintf("<%s><![CDATA[%s]]></%s>", k, v, k))
	}
	xmlStr := fmt.Sprintf("<xml>%s</xml>", buf.String())

	result, err := HTTPSC.PostData(wechat.QueryURL, "text/xml:charset=UTF-8", xmlStr)
	if err != nil {
		return nil, err
	}

	var queryResult common.WeChatQueryResult
	err = xml.Unmarshal(result, &queryResult)
	return &queryResult, err
}
