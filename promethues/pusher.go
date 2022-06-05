package promethues

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

var pushScheduler *PushScheduler

type PushScheduler struct {
	interval time.Duration
	pusher   *push.Pusher
	jobName  string
}

func (ps *PushScheduler) run() {
	t := time.NewTicker(ps.interval)
	for {
		select {
		case <-t.C:
			if err := ps.pusher.Add(); err != nil {
				log.Error(ps.jobName, err)
			}
		}
	}
}

func InitPromethues(Url string, pushInterval time.Duration, jobName string, client *http.Client, cs ...prometheus.Collector) {
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
