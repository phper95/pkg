package main

import (
	"github.com/phper95/pkg/httpclient"
	"github.com/phper95/pkg/prome"
	"github.com/prometheus/client_golang/prometheus"
	"math/rand"
	"time"
)

var (
	Counter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "counter_test",
			Help:        "counter test",
			ConstLabels: prometheus.Labels{"hostname": prome.GetHostName()},
		},
		[]string{"tag"},
	)

	Histogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "histogram_test",
			Help:        "histogram test",
			Buckets:     prome.DefaultBuckets,
			ConstLabels: prometheus.Labels{"machine": prome.GetHostName()},
		},
		[]string{"tag"},
	)
)

func main() {
	url := "192.168.1.86:9091"
	prome.InitPromethues(url, 2*time.Second, "test", httpclient.DefaultClient, Counter, Histogram)

	go testHistogram()
	for i := 0; i < 100; i++ {
		tag := "a"
		if i%2 == 0 {
			tag = "b"
		}
		c, err := Counter.GetMetricWithLabelValues(tag)
		if err != nil {
			prome.PromeStdLogger.Print(err)
		} else {
			c.Inc()
			prome.PromeStdLogger.Print("inc")
			time.Sleep(time.Second)
		}

	}

	time.Sleep(time.Second * 5)
}

func testHistogram() {
	for i := 0; i < 100; i++ {
		tag := "a"
		if i%2 == 0 {
			tag = "b"
		}
		o, err := Histogram.GetMetricWithLabelValues(tag)
		if err != nil {
			prome.PromeStdLogger.Print(err)
		} else {

			//构造随机耗时
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			cost := r.Intn(20)

			o.Observe(float64(cost))
			prome.PromeStdLogger.Print("cost", cost)
			time.Sleep(time.Second)
		}
	}
}
