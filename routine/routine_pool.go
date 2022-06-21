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
	defaultPool = InitPoolWithName("default", numWorkers, maxJobQueueLen, maxJobTimeout)
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

func InitPoolWithName(name string, numWorkers int, maxJobQueueLen int, maxJobTimeout time.Duration) *Pool {
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
	p := InitPoolWithName("default", numWorkers, maxJobQueueLen, maxJobTimeout)
	return p
}

func (p *Pool) QueueLen() int {
	return len(p.JobQueue)
}

func (p *Pool) PutWithTaskName(task *BaseTask) bool {
	return p.put(task)
}

func (p *Pool) Put(f Function) bool {
	return p.put(f)
}

func (p *Pool) PutWait(f Function) {
	if p.stopping {
		routineLogger.Printf("routinepool[%v] was stopping, can not PutWait(task).", p.Name)
		return
	}
	p.checkRunningPanic()

	p.JobQueue <- f
}

func (p *Pool) put(task Task) bool {
	if p.stopping {
		routineLogger.Printf("routinepool[%v] was stopping, can not put(task).", p.Name)
		return false
	}
	p.checkRunningPanic()
	select {
	case p.JobQueue <- task:
		return true
	default:
		routineLogger.Printf("routinepool Put(%s) queue.cap=[%v],len=[%v] is overflowing.",
			p.Name, cap(p.JobQueue), p.QueueLen())
		return false
	}
}

func (p *Pool) reput(task Task) {
	p.JobQueue <- task
}

func (p *Pool) executeJob(task Task, timeout time.Duration) {
	// 如果 大量的 task 长时间执行不结束，
	// 会积压在内存中，使进程总goroutine积压。
	// 这里处理方式是超过4倍workers数，即重新投递任务。
	if p.currGorountine >= int64(p.numWorkers*4) {
		time.Sleep(3 * time.Second) //先缓一下，让任务执行
		p.reput(task)
		routineLogger.Printf("routinepool[%s] numWorkers=[%v] but gorountine[%v] was running, re-put the job.",
			p.Name, p.numWorkers, p.currGorountine)
		return
	}
	var ctx context.Context
	var cancel context.CancelFunc

	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}

	atomic.AddInt64(&p.currGorountine, 1)
	go func() {
		defer atomic.AddInt64(&p.currGorountine, -1)
		//当前gorutine执行完必需cancel context Timeout,才能让下面的select阻塞退出
		if timeout > 0 {
			defer cancel()
		}

		//捕获异常堆栈
		defer func() {
			e := recover()
			if e != nil {
				s := errors.Stack(2)
				log.Fatalf("routinepool[%v] Panic: %v\nTraceback\r:%s",
					p.Name, e, string(s))
			}
		}()

		start := time.Now()
		task.Execute()
		if timeout > 0 && time.Since(start) > timeout {
			routineLogger.Printf("Job runing timeout, limit[%v] used-time[%v] in routinepool[%v]",
				timeout, time.Since(start), p.Name)
		}
	}()

	if timeout > 0 {
		select {
		// timeout时间到了，就会自动ctx.Done()
		case <-ctx.Done():
		}
	}

}

func (p *Pool) Start() {
	if p.running {
		return
	}
	p.running = true
	for i := 0; i < p.numWorkers; i++ {
		go p.run(i)
	}

	time.Sleep(time.Millisecond) //防止start后马上put(task),接着就stop()
}

func (p *Pool) checkRunningPanic() {
	if !p.running {
		msg := fmt.Sprintf("Pool.Start() must be called before run the routinepool[%v].", p.Name)
		routineLogger.Printf(msg)
		panic(msg)
	}
}

func (p *Pool) run(n int) {
	// routineLogger.Printf("worker[%v] start loop.", n)
	defer routineLogger.Printf("routinepool[%v] worker[%v].Done=[%v] exist loop. JobQueue.len=[%v]",
		p.Name, n, p.workers[n].Done, p.QueueLen())

	defer p.wg.Done()
	p.wg.Add(1)
	worker := p.workers[n]
	var stop bool = false
	var stopTime time.Time
	for {
		select {
		case task := <-p.JobQueue:
			p.executeJob(task, p.maxJobTimeout)
			worker.Done += 1
		case stop = <-worker.Stop:
			routineLogger.Printf("routinepool[%v] worker[%v] stop=%v", p.Name, n, stop)
			stopTime = time.Now()
			if !stop {
				close(worker.Stop)
			}
			break
		}

		if stop {
			if p.QueueLen() == 0 {
				routineLogger.Printf("worker[%v] exit-finish, currGorountine=[%v]",
					n, p.currGorountine)
				break
			}

			if time.Since(stopTime) >= p.maxJobTimeout {
				routineLogger.Printf("Exit-timeout[%v] Fail. [%v]jobs was not-finish!!!",
					time.Since(stopTime), p.QueueLen())
				break
			} else {
				routineLogger.Printf("Worker[%v] exiting, [%v]jobs-queue still has-time[%v] to do it.",
					n, p.QueueLen(), p.maxJobTimeout-time.Since(stopTime))
			}
		}
	}
}

func (p *Pool) Stop() {
	p.checkRunningPanic()
	p.stopping = true
	// routineLogger.Printf("Pool.Stop() Called, going to waitgroup")
	for i := 0; i < p.numWorkers; i++ {
		p.workers[i].Stop <- true
		// routineLogger.Printf("Pool.Stop(%v) Called.", i)
	}
	close(p.exit)
	p.wg.Wait()
	// routineLogger.Printf("Pool.Stop() Called end.")
	if p.QueueLen() > 0 {
		routineLogger.Printf("routinepool[%v] when Pool.Stop() had [%v]jobs not-finish.",
			p.Name, p.QueueLen())
	}

	var done int64 = 0
	for i := 0; i < p.numWorkers; i++ {
		done += p.workers[i].Done
	}
	routineLogger.Printf("Stop routine pool", p.Name, "currGorountine", p.currGorountine, "JobQueue len", p.QueueLen())
}

func (p *Pool) StopWait() {
	p.checkRunningPanic()
	p.stopping = true
	for p.QueueLen() > 0 || p.currGorountine > 0 {

		var done int64 = 0
		for i := 0; i < p.numWorkers; i++ {
			done += p.workers[i].Done
			// routineLogger.Printf("Pool.Stop(%v) Called.", i)
		}
		routineLogger.Printf("==--StopWait()--==> routinepool[%v] currGorountine[%v].Done=[%v] JobQueue.len=[%v]",
			p.Name, p.currGorountine, done, p.QueueLen())

		time.Sleep(time.Second * 1)
	}

	p.Stop()
}
