package trace

import (
	"crypto/rand"
	"encoding/hex"
	"go.uber.org/zap"
	"io"
	"sync"
)

const Header = "TRACE-ID"

type T interface {
	ID() string
	WithRequest(req *Request) *Trace
	WithResponse(resp *Response) *Trace
	AppendDialog(dialog *Dialog) *Trace
	AppendSQL(sql *SQL) *Trace
	AppendCache(Cache *Cache) *Trace
	SetLogger(logger *zap.Logger)
	SetAlwaysTrace(b bool)
}

// Trace 记录的参数
type Trace struct {
	mux                sync.Mutex
	Identifier         string      `json:"trace_id"`             // 链路ID
	Request            *Request    `json:"request"`              // 请求信息
	Response           *Response   `json:"response"`             // 返回信息
	ThirdPartyRequests []*Dialog   `json:"third_party_requests"` // 调用第三方接口的信息
	Debugs             []*Debug    `json:"debugs"`               // 调试信息
	SQLs               []*SQL      `json:"sqls"`                 // 执行的 SQL 信息
	Cache              []*Cache    `json:"Cache"`                // 执行的 Cache 信息
	Success            bool        `json:"success"`              // 请求结果 true or false
	CostMillisecond    float64     `json:"cost_millisecond"`     // 执行时长(单位ms)
	Logger             *zap.Logger `json:"-"`
	AlwaysTrace        bool        `json:"always_trace"`
}

// Request 请求信息
type Request struct {
	TTL         string      `json:"ttl"`         // 请求超时时间
	Method      string      `json:"method"`      // 请求方式
	DecodedURL  string      `json:"decoded_url"` // 请求地址
	Header      interface{} `json:"header"`      // 请求 Header 信息
	Body        interface{} `json:"body"`        // 请求 Body 信息
	Logger      *zap.Logger `json:"-"`
	AlwaysTrace bool        `json:"always_trace"`
}

// Response 响应信息
type Response struct {
	Header          interface{} `json:"header"`                      // Header 信息
	Body            interface{} `json:"body"`                        // Body 信息
	BusinessCode    int         `json:"business_code,omitempty"`     // 业务码
	BusinessCodeMsg string      `json:"business_code_msg,omitempty"` // 提示信息
	HttpCode        int         `json:"http_code"`                   // HTTP 状态码
	HttpCodeMsg     string      `json:"http_code_msg"`               // HTTP 状态码信息
	CostMillisecond int64       `json:"cost_millisecond"`            // 执行时间(单位ms)
	Logger          *zap.Logger `json:"-"`
	AlwaysTrace     bool        `json:"always_trace"`
}

type SQL struct {
	TraceTime             string      `json:"trace_time"`              // 时间，格式：2006-01-02 15:04:05
	Stack                 string      `json:"stack"`                   // 文件地址和行号
	SQL                   string      `json:"sql"`                     // SQL 语句
	AffectedRows          int64       `json:"affected_rows"`           // 影响行数
	CostMillisecond       int64       `json:"cost_millisecond"`        // 执行时长(单位ms)
	SlowLoggerMillisecond int64       `json:"slow_logger_millisecond"` //慢查记录时间
	Logger                *zap.Logger `json:"-"`
	AlwaysTrace           bool        `json:"always_trace"`
}

type Cache struct {
	Name                  string      `json:"name"`                    //缓存组件名
	TraceTime             string      `json:"trace_time"`              // 时间，格式：2006-01-02 15:04:05
	CMD                   string      `json:"cmd"`                     // 操作，SET/GET 等
	Key                   string      `json:"key"`                     // Key
	Value                 interface{} `json:"value,omitempty"`         // Value
	TTL                   float64     `json:"ttl,omitempty"`           // 超时时长(单位分)
	CostMillisecond       int64       `json:"cost_millisecond"`        // 执行时长(单位ms)
	SlowLoggerMillisecond int64       `json:"slow_logger_millisecond"` //慢查记录时间
	Logger                *zap.Logger `json:"-"`
	AlwaysTrace           bool        `json:"always_trace"`
}

type D interface {
	AppendResponse(resp *Response)
}

// 内部调用其它方接口的会话信息；失败时会有retry操作，所以 response 会有多次。
type Dialog struct {
	mux             sync.Mutex
	Request         *Request    `json:"request"`          // 请求信息
	Responses       []*Response `json:"responses"`        // 返回信息
	Success         bool        `json:"success"`          // 是否成功，true 或 false
	CostMillisecond int64       `json:"cost_millisecond"` // 执行时长(单位ms)
	Logger          *zap.Logger `json:"-"`
	AlwaysTrace     bool        `json:"always_trace"`
}

// AppendResponse 按转的追加response信息
func (d *Dialog) AppendResponse(resp *Response) {
	if resp == nil {
		return
	}

	d.mux.Lock()
	d.Responses = append(d.Responses, resp)
	d.mux.Unlock()
}

//自定义调试信息
type Debug struct {
	Key             string      `json:"key"`              // 标示
	Value           interface{} `json:"value"`            // 值
	CostMillisecond int64       `json:"cost_millisecond"` // 执行时长(单位ms)
	Logger          *zap.Logger `json:"-"`
}

func New(id string) *Trace {
	if id == "" {
		buf := make([]byte, 10)
		io.ReadFull(rand.Reader, buf)
		id = hex.EncodeToString(buf)
	}

	return &Trace{
		Identifier: id,
	}
}

// ID 唯一标识符
func (t *Trace) ID() string {
	return t.Identifier
}

// WithRequest 设置request
func (t *Trace) WithRequest(req *Request) *Trace {
	t.Request = req
	return t
}

// WithResponse 设置response
func (t *Trace) WithResponse(resp *Response) *Trace {
	t.Response = resp
	return t
}

// AppendDialog 安全的追加内部调用过程dialog
func (t *Trace) AppendDialog(dialog *Dialog) *Trace {
	if dialog == nil {
		return t
	}

	t.mux.Lock()
	defer t.mux.Unlock()

	t.ThirdPartyRequests = append(t.ThirdPartyRequests, dialog)
	return t
}

// AppendDebug 追加 debug
func (t *Trace) AppendDebug(debug *Debug) *Trace {
	if debug == nil {
		return t
	}

	t.mux.Lock()
	defer t.mux.Unlock()

	t.Debugs = append(t.Debugs, debug)
	return t
}

// AppendSQL 追加 SQL
func (t *Trace) AppendSQL(sql *SQL) *Trace {
	if sql == nil {
		return t
	}

	t.mux.Lock()
	defer t.mux.Unlock()

	t.SQLs = append(t.SQLs, sql)
	return t
}

//设置日志
func (t *Trace) SetLogger(logger *zap.Logger) {
	t.Logger = logger
}

//始终记录trace信息
func (t *Trace) SetAlwaysTrace(b bool) {
	t.AlwaysTrace = true
}

// AppendCache 追加 Cache
func (t *Trace) AppendCache(Cache *Cache) *Trace {
	if Cache == nil {
		return t
	}

	t.mux.Lock()
	defer t.mux.Unlock()

	t.Cache = append(t.Cache, Cache)
	return t
}
