package routine

import (
	"gitee.com/phper95/pkg/errors"
	"log"
	"sync"
	"time"
)

type SeqExecutor struct {
	chQueues   []chan Function
	stopSignal []int
	workers    int
	queueLen   float64
	timeout    time.Duration

	wg sync.WaitGroup
}

// 最坏情况，退出时阻碍timeout的时间，留给待队列中的任务去执行。
func NewSeqExecutor(
	workers int,
	perQueueLen int,
	maxTimeout time.Duration) *SeqExecutor {

	seq := &SeqExecutor{
		timeout:  maxTimeout,
		queueLen: float64(perQueueLen),
		workers:  workers,
		wg:       sync.WaitGroup{},
	}
	seq.stopSignal = make([]int, workers)
	seq.chQueues = make([]chan Function, workers)
	for i := 0; i < workers; i++ {
		seq.chQueues[i] = make(chan Function, perQueueLen)
		seq.stopSignal[i] = 0
	}

	routineLogger.Print("NewSeqExecutor w[%v] perQueueLen[%v] timeout[%v]",
		workers, perQueueLen, maxTimeout)
	return seq
}

func (self *SeqExecutor) execute(f Function) {
	// TODO: timeout
	defer errors.Recover()
	f()
}

func (self *SeqExecutor) worker_loop(n int) {
	defer self.wg.Done()
	self.wg.Add(1)

	var queue = self.chQueues[n]
	var stop int = 0
	var stopTime time.Time
	for {
		stop = self.stopSignal[n]
		select {
		case f := <-queue:
			self.execute(f)
		}
		if stop != 0 {
			if len(queue) == 0 {
				// routineLogger.Print("[%v]worker Exit successful,all jobs was finish.", n)
				return
			}

			if stopTime.IsZero() {
				stopTime = time.Now()
			}

			if time.Since(stopTime) >= self.timeout {
				log.Fatal("Exit-timeout[%v] Fail,[%v]jobs not-finish.",
					time.Since(stopTime), len(queue))
				return
			} else {
				continue
				// routineLogger.Print("Exiting, [%v]jobs was doing.", len(queue))
			}

		} // end if stop
	} //end loop-for
}

func (self *SeqExecutor) Start() {
	for i := 0; i < self.workers; i++ {
		go self.worker_loop(i)
	}
}

// 队列满了时，会阻塞
func (self *SeqExecutor) Put(f Function, hash int64) {
	n := int(hash) % self.workers
	q := self.chQueues[n]
	if float64(len(q))/self.queueLen >= 0.85 {
		routineLogger.Print("Job Queue using more then 85%%,cap[%v] len[%v]",
			cap(q), len(q))
	}

	// 阻塞
	q <- f
	// 丢弃
	// select {
	// case q <- f:
	// default:
	// 	routineLogger.Print("Job Queue full, drop Jobs.")
	// }
}

// 最坏情况会阻碍timeout的时间，留给待队列中的任务去执行。
func (self *SeqExecutor) Stop() {
	for i := 0; i < self.workers; i++ {
		self.stopSignal[i] = 1
	}

	routineLogger.Print("SeqExecutor.stop() -> WaitGroup ...")
	self.wg.Wait()
	routineLogger.Print("SeqExecutor stop finish.")
}
