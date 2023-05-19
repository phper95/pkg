package routine

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRoutinePoolPanic(t *testing.T) {
	numWorkers, jobQueueLen := 1, 2
	InitPoolWithName(PoolNameDefault, numWorkers, jobQueueLen, time.Second)

	panicFunc := func() {
		var err error
		err.Error()
	}
	GetPool(PoolNameDefault).Put(panicFunc)
	success := false
	routineFunc := func() {
		success = true
		routineLogger.Printf("panicFunc exec")
	}
	GetPool(PoolNameDefault).Put(routineFunc)
	time.Sleep(time.Millisecond * 50)
	if !success {
		t.Fatalf("routineFunc exec failed")
	}
	GetPool(PoolNameDefault).Stop()
}

func TestRoutinePool(t *testing.T) {

	numWorkers, jobQueueLen := 10, 100
	jobTimeout := time.Duration(0)
	routinePool := InitPoolWithName("test", numWorkers, jobQueueLen, jobTimeout)
	for i := 0; i < 10; i++ {
		cur := i
		routinePool.Put(func() {
			fmt.Println(cur)
		})
	}

	time.Sleep(5 * time.Second)
}

func TestRoutineTimeout(t *testing.T) {
	numWorkers, jobQueueLen := 2, 4
	jobTimeout := time.Second / 100
	grp := InitPoolWithName("timeout-job", numWorkers, jobQueueLen, jobTimeout)

	l := sync.Mutex{}
	jobCnt := 0
	timeout_job := func() {
		l.Lock()
		jobCnt++
		l.Unlock()
		time.Sleep(jobTimeout * 4) // 4倍的最大任务超时
		routineLogger.Printf("Long-Time Job Done -------cnt=%v--", jobCnt)
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
}
