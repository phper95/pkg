package httpclient

import (
	"net"
	"net/http"
	"time"
)

const (
	DefaultClient    = "default"
	MetricPushClient = "metric-push-client"
)

var clientPools map[string]*http.Client

func init() {
	clientPools = make(map[string]*http.Client)
	clientPools[DefaultClient] = createDefaultClient()
	clientPools[MetricPushClient] = createMetricPushClient()
}

func createDefaultClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   20 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}
}

func createMetricPushClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns:          500,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}
}
func GetHttpClient(name string) *http.Client {
	if name == "" {
		name = DefaultClient
	}

	if client, ok := clientPools[name]; ok {
		return client
	} else {
		clientPools[name] = createDefaultClient()
		return clientPools[name]
	}
}
