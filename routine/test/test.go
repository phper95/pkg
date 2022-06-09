package main

import (
	"fmt"
	"runtime"
	"sync"
)

func main() {
	getGoroutineMemConsume()
}
func getGoroutineMemConsume() {
	var c chan int
	var wg sync.WaitGroup
	const goroutineNum = 1e4 // 1 * 10^4

	memConsumed := func() uint64 {
		runtime.GC() //GC，排除对象影响
		var memStat runtime.MemStats
		runtime.ReadMemStats(&memStat)
		return memStat.Sys
	}

	noop := func() {
		wg.Done()
		<-c //防止goroutine退出，内存被释放
	}

	wg.Add(goroutineNum)
	before := memConsumed() //获取创建goroutine前内存
	for i := 0; i < goroutineNum; i++ {
		go noop()
	}
	wg.Wait()
	after := memConsumed() //获取创建goroutine后内存

	fmt.Printf("%.3f KB\n", float64(after-before)/goroutineNum/1000)
}
