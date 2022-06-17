package routine

import (
	"sync"
	"testing"
	"time"
)

func Test_grpool_Panic(t *testing.T) {
	numWorkers, jobQueueLen := 1, 2
	Init(numWorkers, jobQueueLen, time.Second)

	panic_nil_pointer_func := func() {
		var err error
		err.Error()
	}
	PutTask(panic_nil_pointer_func)
	success := false
	t_success := func() {
		success = true
		routineLogger.Print("============> set success=Ture")
	}
	PutTask(t_success)
	time.Sleep(time.Millisecond * 50)
	if !success {
		t.Fatalf("must support function-job painc.")
	}
	Stop()
}

func Test_grpool_release(t *testing.T) {

	numWorkers, jobQueueLen := 1, 64
	jobTimeout := time.Second / 100
	Init(numWorkers, jobQueueLen, jobTimeout)

	start := time.Now()

	double_timeout_job := func() {
		time.Sleep(jobTimeout * 2) // 2倍的最大任务超时, 必然不打印以下"Job Done"
		// time.Sleep(jobTimeout / 2) // 1/2的最大任务超时，一定要打印出"Job Done"
		// t.Fatalf("job run use time was max-timeout*2, do not Print this msg")
	}

	PutTask(double_timeout_job)

	Stop()

	if time.Since(start) >= 2*jobTimeout {
		t.Fatalf("job-max-execout-timeout was set[%v]", jobTimeout)
	}

	routineLogger.Print("End Testing: QueueLen=[%v]", QueueLen())

}

func Test_manay_goruntine(t *testing.T) {
	numWorkers, jobQueueLen := 2, 4
	jobTimeout := time.Second / 100
	grp := NewPoolWithName("Long", numWorkers, jobQueueLen, jobTimeout)
	grp.Start()

	l := sync.Mutex{}
	jobCnt := 0
	timeout_job := func() {
		l.Lock()
		jobCnt++
		l.Unlock()
		time.Sleep(jobTimeout * 4) // 4倍的最大任务超时
		routineLogger.Print("Long-Time Job Done -------cnt=%v--", jobCnt)
	}
	expect := 0
	for i := 0; i < numWorkers*jobQueueLen; i++ {
		grp.PutWait(timeout_job)
		expect++
	}

	grp.StopWait()
	if expect != jobCnt {
		t.Fatalf("expect[%v] all job was done, but got[%v],", expect, jobCnt)
	}
	// time.Sleep(jobTimeout * 5)

	// log.Close()
}
