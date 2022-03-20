package httpclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"gitee.com/phper95/pkg/errors"
	"gitee.com/phper95/pkg/trace"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	httpURL "net/url"
	"time"
)

const (
	// DefaultTTL 一次http请求最长执行1分钟
	DefaultTTL = time.Minute
)

var DefaultClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives:  true,
		DisableCompression: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		MaxIdleConns:        100,
		MaxConnsPerHost:     100,
		MaxIdleConnsPerHost: 100,
	},
}

// Get get 请求
func Get(url string, form httpURL.Values, options ...Option) (httpCode int, body []byte, err error) {
	return withoutBody(http.MethodGet, url, form, options...)
}

// Delete delete 请求
func Delete(url string, form httpURL.Values, options ...Option) (httpCode int, body []byte, err error) {
	return withoutBody(http.MethodDelete, url, form, options...)
}

func doHTTP(ctx context.Context, method, url string, payload []byte, opt *option) ([]byte, int, error) {
	ts := time.Now()

	if mock := opt.mock; mock != nil {
		if opt.dialog != nil {
			opt.dialog.AppendResponse(&trace.Response{
				HttpCode:        http.StatusOK,
				HttpCodeMsg:     http.StatusText(http.StatusOK),
				Body:            string(mock()),
				CostMillisecond: time.Since(ts).Milliseconds(),
			})
		}
		return mock(), http.StatusOK, nil
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
	if err != nil {
		return nil, -1, errors.Wrapf(err, "new request [%s %s] err", method, url)
	}

	for key, value := range opt.header {
		req.Header.Set(key, value[0])
	}

	resp, err := DefaultClient.Do(req)
	if err != nil {
		err = errors.Wrapf(err, "do request [%s %s] err", method, url)
		if opt.dialog != nil {
			opt.dialog.AppendResponse(&trace.Response{
				Body:            err.Error(),
				CostMillisecond: time.Since(ts).Milliseconds(),
			})
		}

		if opt.logger != nil {
			opt.logger.Warn("doHTTP got err", zap.Error(err))
		}
		return nil, _StatusDoReqErr, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.Wrapf(err, "read resp body from [%s %s] err", method, url)
		if opt.dialog != nil {
			opt.dialog.AppendResponse(&trace.Response{
				Body:            err.Error(),
				CostMillisecond: time.Since(ts).Milliseconds(),
			})
		}

		if opt.logger != nil {
			opt.logger.Warn("doHTTP got err", zap.Error(err))
		}
		return nil, _StatusReadRespErr, err
	}

	defer func() {
		if opt.dialog != nil {
			opt.dialog.AppendResponse(&trace.Response{
				Header:          resp.Header,
				HttpCode:        resp.StatusCode,
				HttpCodeMsg:     resp.Status,
				Body:            string(body), // unsafe
				CostMillisecond: time.Since(ts).Milliseconds(),
			})
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, newReplyErr(
			resp.StatusCode,
			body,
			errors.Errorf("do [%s %s] return code: %d message: %s", method, url, resp.StatusCode, string(body)),
		)
	}

	return body, http.StatusOK, nil
}

func withoutBody(method, url string, form httpURL.Values, options ...Option) (httpCode int, body []byte, err error) {
	if url == "" {
		err = errors.New("url required")
		return
	}

	if len(form) > 0 {
		if url, err = formValues2URL(url, form); err != nil {
			return
		}
	}

	ts := time.Now()

	opt := getOption()
	defer func() {
		if opt.trace != nil {
			opt.dialog.Success = err == nil
			opt.dialog.CostMillisecond = time.Since(ts).Milliseconds()
			opt.trace.AppendDialog(opt.dialog)
		}

		releaseOption(opt)
	}()

	for _, f := range options {
		if f != nil {
			f(opt)
		}

	}
	opt.header["Content-Type"] = []string{"application/x-www-form-urlencoded; charset=utf-8"}
	if opt.trace != nil {
		opt.header[trace.Header] = []string{opt.trace.ID()}
	}

	ttl := opt.ttl
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	ctx, cancel := context.WithTimeout(context.Background(), ttl)
	defer cancel()

	if opt.dialog != nil {
		decodedURL, _ := httpURL.QueryUnescape(url)
		opt.dialog.Request = &trace.Request{
			TTL:        ttl.String(),
			Method:     method,
			DecodedURL: decodedURL,
			Header:     opt.header,
		}
	}

	retryTimes := opt.retryTimes
	if retryTimes <= 0 {
		retryTimes = DefaultRetryTimes
	}

	retryDelay := opt.retryDelay
	if retryDelay <= 0 {
		retryDelay = DefaultRetryDelay
	}

	defer func() {
		if opt.logger == nil {
			return
		}
		info := &struct {
			TraceID string `json:"trace_id"`
			Request struct {
				Method string `json:"method"`
				URL    string `json:"url"`
			} `json:"request"`
			Response struct {
				HTTPCode int    `json:"http_code"`
				Body     string `json:"body"`
			} `json:"response"`
			Error string `json:"error"`
		}{}

		if opt.trace != nil {
			info.TraceID = opt.trace.ID()
		}
		info.Request.Method = method
		info.Request.URL = url
		info.Response.HTTPCode = httpCode
		info.Response.Body = string(body)
		info.Error = ""
		if err != nil {
			info.Error = fmt.Sprintf("%+v", err)
		}

		raw, _ := json.MarshalIndent(info, "", " ")
		opt.logger.Warn(string(raw))

	}()

	for k := 0; k < retryTimes; k++ {
		body, httpCode, err = doHTTP(ctx, method, url, nil, opt)
		if shouldRetry(ctx, httpCode) || (opt.retryVerify != nil && opt.retryVerify(body)) {
			time.Sleep(retryDelay)
			continue
		}

		return
	}
	return
}

// PostForm post form 请求
func PostForm(url string, form httpURL.Values, options ...Option) (httpCode int, body []byte, err error) {
	return withFormBody(http.MethodPost, url, form, options...)
}

// PostJSON post json 请求
func PostJSON(url string, raw json.RawMessage, options ...Option) (httpCode int, body []byte, err error) {
	return withJSONBody(http.MethodPost, url, raw, options...)
}

// PutForm put form 请求
func PutForm(url string, form httpURL.Values, options ...Option) (httpCode int, body []byte, err error) {
	return withFormBody(http.MethodPut, url, form, options...)
}

// PutJSON put json 请求
func PutJSON(url string, raw json.RawMessage, options ...Option) (httpCode int, body []byte, err error) {
	return withJSONBody(http.MethodPut, url, raw, options...)
}

// PatchFrom patch form 请求
func PatchFrom(url string, form httpURL.Values, options ...Option) (httpCode int, body []byte, err error) {
	return withFormBody(http.MethodPatch, url, form, options...)
}

// PatchJSON patch json 请求
func PatchJSON(url string, raw json.RawMessage, options ...Option) (httpCode int, body []byte, err error) {
	return withJSONBody(http.MethodPatch, url, raw, options...)
}

func withFormBody(method, url string, form httpURL.Values, options ...Option) (httpCode int, body []byte, err error) {
	if url == "" {
		err = errors.New("url required")
		return
	}
	if len(form) == 0 {
		err = errors.New("form required")
		return
	}

	ts := time.Now()

	opt := getOption()
	defer func() {
		if opt.trace != nil {
			opt.dialog.Success = err == nil
			opt.dialog.CostMillisecond = time.Since(ts).Milliseconds()
			opt.trace.AppendDialog(opt.dialog)
		}

		releaseOption(opt)
	}()

	for _, f := range options {
		if f != nil {
			f(opt)
		}
	}
	opt.header["Content-Type"] = []string{"application/x-www-form-urlencoded; charset=utf-8"}
	if opt.trace != nil {
		opt.header[trace.Header] = []string{opt.trace.ID()}
	}

	ttl := opt.ttl
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	ctx, cancel := context.WithTimeout(context.Background(), ttl)
	defer cancel()

	formValue := form.Encode()
	if opt.dialog != nil {
		decodedURL, _ := httpURL.QueryUnescape(url)
		opt.dialog.Request = &trace.Request{
			TTL:        ttl.String(),
			Method:     method,
			DecodedURL: decodedURL,
			Header:     opt.header,
			Body:       formValue,
		}
	}

	retryTimes := opt.retryTimes
	if retryTimes <= 0 {
		retryTimes = DefaultRetryTimes
	}

	retryDelay := opt.retryDelay
	if retryDelay <= 0 {
		retryDelay = DefaultRetryDelay
	}

	defer func() {
		if opt.logger == nil {
			return
		}
		info := &struct {
			TraceID string `json:"trace_id"`
			Request struct {
				Method string `json:"method"`
				URL    string `json:"url"`
			} `json:"request"`
			Response struct {
				HTTPCode int    `json:"http_code"`
				Body     string `json:"body"`
			} `json:"response"`
			Error string `json:"error"`
		}{}

		if opt.trace != nil {
			info.TraceID = opt.trace.ID()
		}
		info.Request.Method = method
		info.Request.URL = url
		info.Response.HTTPCode = httpCode
		info.Response.Body = string(body)
		info.Error = ""
		if err != nil {
			info.Error = fmt.Sprintf("%+v", err)
		}

		raw, _ := json.MarshalIndent(info, "", " ")
		opt.logger.Warn(string(raw))

	}()

	for k := 0; k < retryTimes; k++ {
		body, httpCode, err = doHTTP(ctx, method, url, []byte(formValue), opt)
		if shouldRetry(ctx, httpCode) || (opt.retryVerify != nil && opt.retryVerify(body)) {
			time.Sleep(retryDelay)
			continue
		}

		return
	}
	return
}

func withJSONBody(method, url string, raw json.RawMessage, options ...Option) (httpCode int, body []byte, err error) {
	if url == "" {
		err = errors.New("url required")
		return
	}
	if len(raw) == 0 {
		err = errors.New("raw required")
		return
	}

	ts := time.Now()

	opt := getOption()
	defer func() {
		if opt.trace != nil {
			opt.dialog.Success = err == nil
			opt.dialog.CostMillisecond = time.Since(ts).Milliseconds()
			opt.trace.AppendDialog(opt.dialog)
		}

		releaseOption(opt)
	}()

	for _, f := range options {
		if f != nil {
			f(opt)
		}
	}
	opt.header["Content-Type"] = []string{"application/json; charset=utf-8"}
	if opt.trace != nil {
		opt.header[trace.Header] = []string{opt.trace.ID()}
	}

	ttl := opt.ttl
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	ctx, cancel := context.WithTimeout(context.Background(), ttl)
	defer cancel()

	if opt.dialog != nil {
		decodedURL, _ := httpURL.QueryUnescape(url)
		opt.dialog.Request = &trace.Request{
			TTL:        ttl.String(),
			Method:     method,
			DecodedURL: decodedURL,
			Header:     opt.header,
			Body:       string(raw), // TODO unsafe
		}
	}

	retryTimes := opt.retryTimes
	if retryTimes <= 0 {
		retryTimes = DefaultRetryTimes
	}

	retryDelay := opt.retryDelay
	if retryDelay <= 0 {
		retryDelay = DefaultRetryDelay
	}

	defer func() {

		info := &struct {
			TraceID string `json:"trace_id"`
			Request struct {
				Method string `json:"method"`
				URL    string `json:"url"`
			} `json:"request"`
			Response struct {
				HTTPCode int    `json:"http_code"`
				Body     string `json:"body"`
			} `json:"response"`
			Error string `json:"error"`
		}{}

		if opt.trace != nil {
			info.TraceID = opt.trace.ID()
		}
		info.Request.Method = method
		info.Request.URL = url
		info.Response.HTTPCode = httpCode
		info.Response.Body = string(body)
		info.Error = ""
		if err != nil {
			info.Error = fmt.Sprintf("%+v", err)
		}

		raw, _ := json.MarshalIndent(info, "", " ")
		opt.logger.Warn(string(raw))

	}()

	for k := 0; k < retryTimes; k++ {
		body, httpCode, err = doHTTP(ctx, method, url, raw, opt)
		if shouldRetry(ctx, httpCode) || (opt.retryVerify != nil && opt.retryVerify(body)) {
			time.Sleep(retryDelay)
			continue
		}

		return
	}
	return
}

func formValues2URL(rawURL string, form httpURL.Values) (string, error) {
	if rawURL == "" {
		return "", errors.New("rawURL required")
	}
	if len(form) == 0 {
		return "", errors.New("form required")
	}

	target, err := httpURL.Parse(rawURL)
	if err != nil {
		return "", errors.Wrapf(err, "parse rawURL `%s` err", rawURL)
	}

	urlValues := target.Query()
	for key, values := range form {
		for _, value := range values {
			urlValues.Add(key, value)
		}
	}

	target.RawQuery = urlValues.Encode()
	return target.String(), nil
}
