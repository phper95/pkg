package routine

import (
	"context"
	"fmt"
	"gitee.com/phper95/pkg/errors"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var routineLogger stdLogger

func init() {
	routineLogger = log.New(os.Stdout, "[Gorm] ", log.LstdFlags|log.Lshortfile)
}

var defaultPool *Pool

type Task interface {
	GetTaskName() string
	Execute()
}

type Function func()

func (f Function) GetTaskName() string {
	return "unknown"
}

func (f Function) Execute() {
	f()
}

type BaseTask struct {
	Name string
	F    Function
}

func (t *BaseTask) GetTaskName() string {
	return t.Name
}

func (t *BaseTask) Execute() {
	t.F()
}

func Init(numWorkers int, maxJobQueueLen int, maxJobTimeout time.Duration) {
	defaultPool = NewPoolWithName("default", numWorkers, maxJobQueueLen, maxJobTimeout)
	defaultPool.Start()
}

func PutTask(f Function) {
	if defaultPool == nil {
		Init(8, 64, 10*time.Second)
	}
	defaultPool.Put(f)
}

func Stop() {
	if defaultPool == nil {
		return
	}
	defaultPool.Stop()
	defaultPool = nil
}

func QueueLen() int {
	if defaultPool == nil {
		return 0
	}
	return defaultPool.QueueLen()
}

type worker struct {
	Stop chan bool
	Done int64
}

type Pool struct {
	Name           string
	JobQueue       chan Task
	workers        []*worker
	numWorkers     int
	maxJobTimeout  time.Duration
	wg             sync.WaitGroup
	currGorountine int64
	exit           chan bool
	stopping       bool
	running        bool
}

func NewPoolWithName(name string, numWorkers int, maxJobQueueLen int, maxJobTimeout time.Duration) *Pool {
	p := &Pool{
		Name:          name,
		JobQueue:      make(chan Task, maxJobQueueLen),
		workers:       make([]*worker, numWorkers),
		numWorkers:    numWorkers,
		maxJobTimeout: maxJobTimeout,
		exit:          make(chan bool, 1),
	}
	for i := 0; i < numWorkers; i++ {
		p.workers[i] = &worker{make(chan bool, 1), 0}
	}
	return p
}

func NewPool(numWorkers int, maxJobQueueLen int, maxJobTimeout time.Duration) *Pool {
	p := NewPoolWithName("default", numWorkers, maxJobQueueLen, maxJobTimeout)
	return p
}

func (self *Pool) QueueLen() int {
	return len(self.JobQueue)
}

func (self *Pool) PutWithTaskName(task *BaseTask) bool {
	return self.put(task)
}

func (self *Pool) Put(f Function) bool {
	return self.put(f)
}

func (self *Pool) PutWait(f Function) {
	if self.stopping {
		routineLogger.Print("grpool[%v] was stopping, can not PutWait(task).", self.Name)
		return
	}
	self.if_not_running_panic()

	self.JobQueue <- f
}

func (self *Pool) put(task Task) bool {
	if self.stopping {
		routineLogger.Print("grpool[%v] was stopping, can not put(task).", self.Name)
		return false
	}
	self.if_not_running_panic()
	select {
	case self.JobQueue <- task:
		return true
	default:
		routineLogger.Print("grpool Put(%s) queue.cap=[%v],len=[%v] is overflowing.",
			self.Name, cap(self.JobQueue), self.QueueLen())
		return false
	}
}

func (self *Pool) reput(task Task) {
	self.JobQueue <- task
}

func (self *Pool) executeJob(task Task, timeout time.Duration) {
	// 如果 大量的 task 长时间执行不结束，
	// 会积压在内存中，使进程总goroutine积压。
	// 这里处理方式是超过4倍workers数，即重新投递任务。
	if self.currGorountine >= int64(self.numWorkers*4) {
		time.Sleep(timeout / 10) //先缓一下，让任务执行
		self.reput(task)
		routineLogger.Print("grpool[%s] numWorkers=[%v] but gorountine[%v] was running, re-put the job.",
			self.Name, self.numWorkers, self.currGorountine)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	atomic.AddInt64(&self.currGorountine, 1)
	go func() { // define function For recover() and cancel()
		// f() 执行完，这里必需要cancel一下，才能让下面的select阻塞退出
		defer atomic.AddInt64(&self.currGorountine, -1)
		defer cancel()
		defer func() {
			e := recover()
			if e != nil {
				s := errors.Stack(2)
				log.Fatal("grpool[%v] Panic: %v\nTraceback\r:%s",
					self.Name, e, string(s))
			}
		}()

		start := time.Now()
		task.Execute()
		if time.Since(start) > timeout {
			routineLogger.Print("Job runing timeout, limit[%v] used-time[%v] in grpool[%v]",
				timeout, time.Since(start), self.Name)
		}

		//if grpoolMetrics != nil {
		//	grpoolMetrics.updateDelayHistogram(self.Name, task.GetTaskName(), start)
		//}
	}()

	select {
	// timeout时间到了，就会自动ctx.Done()
	case <-ctx.Done():
	}
}

func (self *Pool) Start() {
	if self.running {
		return
	}
	self.running = true
	for i := 0; i < self.numWorkers; i++ {
		go self.worker_loop(i)
	}

	time.Sleep(time.Millisecond) //防止start后马上put(task),接着就stop()
}

func (self *Pool) if_not_running_panic() {
	if !self.running {
		msg := fmt.Sprintf("PLS call the Pool.Start() to run the grpool[%v].", self.Name)
		routineLogger.Print(msg)
		panic(msg)
	}
}

func (self *Pool) worker_loop(n int) {
	// routineLogger.Print("worker[%v] start loop.", n)
	defer routineLogger.Print("grpool[%v] worker[%v].Done=[%v] exist loop. JobQueue.len=[%v]",
		self.Name, n, self.workers[n].Done, self.QueueLen())

	defer self.wg.Done()
	self.wg.Add(1)
	worker := self.workers[n]
	var stop bool = false
	var stopTime time.Time
	for {
		select {
		case task := <-self.JobQueue:
			self.executeJob(task, self.maxJobTimeout)
			worker.Done += 1
		case stop = <-worker.Stop:
			routineLogger.Print("grpool[%v] worker[%v] stop=%v", self.Name, n, stop)
			stopTime = time.Now()
			if !stop {
				close(worker.Stop)
			}
			break
		}

		if stop {
			if self.QueueLen() == 0 {
				routineLogger.Print("worker[%v] exit-finish, currGorountine=[%v]",
					n, self.currGorountine)
				break
			}

			if time.Since(stopTime) >= self.maxJobTimeout {
				routineLogger.Print("Exit-timeout[%v] Fail. [%v]jobs was not-finish.",
					time.Since(stopTime), self.QueueLen())
				break
			} else {
				routineLogger.Print("Worker[%v] exiting, [%v]jobs-queue still has-time[%v] to do it.",
					n, self.QueueLen(), self.maxJobTimeout-time.Since(stopTime))
			}
		} //end if stop
	} //end for-loop
}

func (self *Pool) Stop() {
	self.if_not_running_panic()
	self.stopping = true
	// routineLogger.Print("Pool.Stop() Called, going to waitgroup")
	for i := 0; i < self.numWorkers; i++ {
		self.workers[i].Stop <- true
		// routineLogger.Print("Pool.Stop(%v) Called.", i)
	}
	close(self.exit)
	self.wg.Wait()
	// routineLogger.Print("Pool.Stop() Called end.")
	if self.QueueLen() > 0 {
		routineLogger.Print("grpool[%v] when Pool.Stop() had [%v]jobs not-finish.",
			self.Name, self.QueueLen())
	}

	var done int64 = 0
	for i := 0; i < self.numWorkers; i++ {
		done += self.workers[i].Done
	}
	routineLogger.Print("Stop routine pool", self.Name, "currGorountine", self.currGorountine, "JobQueue len", self.QueueLen())
}

func (self *Pool) StopWait() {
	self.if_not_running_panic()
	self.stopping = true
	// close(self.JobQueue)
	for self.QueueLen() > 0 || self.currGorountine > 0 {

		var done int64 = 0
		for i := 0; i < self.numWorkers; i++ {
			done += self.workers[i].Done
			// routineLogger.Print("Pool.Stop(%v) Called.", i)
		}
		routineLogger.Print("==--StopWait()--==> grpool[%v] currGorountine[%v].Done=[%v] JobQueue.len=[%v]",
			self.Name, self.currGorountine, done, self.QueueLen())

		time.Sleep(time.Second * 1)
	}

	self.Stop()
}
