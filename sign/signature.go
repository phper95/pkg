package sign

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"gitee.com/phper95/pkg/timeutil"
	"github.com/pkg/errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var _ Signature = (*signature)(nil)

const (
	delimiter = "|"
)

// 合法的 Methods
var methods = map[string]bool{
	http.MethodGet:     true,
	http.MethodPost:    true,
	http.MethodHead:    true,
	http.MethodPut:     true,
	http.MethodPatch:   true,
	http.MethodDelete:  true,
	http.MethodConnect: true,
	http.MethodOptions: true,
	http.MethodTrace:   true,
}

type Signature interface {

	// Generate 生成签名
	Generate(path string, method string, params url.Values) (authorization, date string, err error)

	// Verify 验证签名
	Verify(authorization, date string, path string, method string, params url.Values) (ok bool, err error)
}

type signature struct {
	key    string
	secret string
	ttl    time.Duration
}

func New(key, secret string, ttl time.Duration) Signature {
	return &signature{
		key:    key,
		secret: secret,
		ttl:    ttl,
	}
}

// Generate
// path 请求的路径 (不附带 querystring)
func (s *signature) Generate(path string, method string, params url.Values) (authorization, date string, err error) {
	if path == "" {
		err = errors.New("path required")
		return
	}

	if method == "" {
		err = errors.New("method required")
		return
	}

	methodName := strings.ToUpper(method)
	if !methods[methodName] {
		err = errors.New("method param error")
		return
	}

	// Date
	date = timeutil.CSTLayoutString()

	// Encode() 方法中自带 sorted by key
	sortParamsEncode, err := url.QueryUnescape(params.Encode())
	if err != nil {
		err = errors.Errorf("url QueryUnescape error %v", err)
		return
	}

	// 加密字符串规则
	buffer := bytes.NewBuffer(nil)
	buffer.WriteString(path)
	buffer.WriteString(delimiter)
	buffer.WriteString(methodName)
	buffer.WriteString(delimiter)
	buffer.WriteString(sortParamsEncode)
	buffer.WriteString(delimiter)
	buffer.WriteString(date)

	// 对数据进行 sha256 加密，并进行 base64 encode
	hash := hmac.New(sha256.New, []byte(s.secret))
	hash.Write(buffer.Bytes())
	digest := base64.StdEncoding.EncodeToString(hash.Sum(nil))

	authorization = fmt.Sprintf("%s %s", s.key, digest)
	return
}

func (s *signature) Verify(authorization, date string, path string, method string, params url.Values) (ok bool, err error) {
	if date == "" {
		err = errors.New("date required")
		return
	}

	if path == "" {
		err = errors.New("path required")
		return
	}

	if method == "" {
		err = errors.New("method required")
		return
	}

	methodName := strings.ToUpper(method)
	if !methods[methodName] {
		err = errors.New("method param error")
		return
	}

	ts, err := timeutil.ParseCSTInLocation(date)
	if err != nil {
		err = errors.New("date must follow '2006-01-02 15:04:05'")
		return
	}

	if timeutil.SubInLocation(ts) > float64(s.ttl/time.Second) {
		err = errors.Errorf("date exceeds limit %v", s.ttl)
		return
	}

	// Encode() 方法中自带 sorted by key
	sortParamsEncode, err := url.QueryUnescape(params.Encode())
	if err != nil {
		err = errors.Errorf("url QueryUnescape error %v", err)
		return
	}
	buffer := bytes.NewBuffer(nil)
	buffer.WriteString(path)
	buffer.WriteString(delimiter)
	buffer.WriteString(methodName)
	buffer.WriteString(delimiter)
	buffer.WriteString(sortParamsEncode)
	buffer.WriteString(delimiter)
	buffer.WriteString(date)

	// 对数据进行 hmac 加密，并进行 base64 encode
	hash := hmac.New(sha256.New, []byte(s.secret))
	hash.Write(buffer.Bytes())
	digest := base64.StdEncoding.EncodeToString(hash.Sum(nil))

	ok = authorization == fmt.Sprintf("%s %s", s.key, digest)
	fmt.Println("authorization", authorization, "auth", fmt.Sprintf("%s %s", s.key, digest))
	return
}
