package prome

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"log"
	"net/http"
	"os"
	"time"
)

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

const (
	DefaultPushInterval = 60 * time.Second
)

var (
	pushScheduler  *PushScheduler
	PromeStdLogger stdLogger
	DefaultBuckets = []float64{5, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 1000, 1500, 2000, 2500, 3000, 4000, 5000, 6000, 7000, 8000, 10000, 20000, 30000}
)

type PushScheduler struct {
	interval time.Duration
	pusher   *push.Pusher
	jobName  string
}

func init() {
	PromeStdLogger = log.New(os.Stdout, "[Prome] ", log.LstdFlags|log.Lshortfile)
}

func (ps *PushScheduler) run() {
	t := time.NewTicker(ps.interval)
	for {
		select {
		case <-t.C:
			if err := ps.pusher.Add(); err != nil {
				PromeStdLogger.Print(ps.jobName, err)
			}
		}
	}
}

func InitPromethues(Url string, pushInterval time.Duration, jobName string, client *http.Client, cs ...prometheus.Collector) {
	if pushInterval == 0 {
		pushInterval = DefaultPushInterval
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(cs...)

	pusher := push.New(Url, jobName).Gatherer(registry)
	if client != nil {
		pusher.Client(client)
	}

	pushScheduler = &PushScheduler{
		interval: pushInterval,
		pusher:   pusher,
		jobName:  jobName,
	}
	go pushScheduler.run()
}

func GetHostName() string {
	name, err := os.Hostname()
	if err != nil {
		PromeStdLogger.Print(err)
	}
	return name
}
