package routine

import (
	"runtime/debug"
	"sync"
	"time"
)

const RoutinePoolCloseTimeOut = 60 * time.Second

// DefaultMaxConcurrence 默认最大并发数
var DefaultMaxConcurrence int

// RoutinePool 协程池
type RoutinePool struct {
	lockChan       chan int
	funcs          chan func()
	MaxConcurrence int //最大并发数
	Name           string
	MaxPoolSize    int // 协程池最大容量
}

var pools map[string]*RoutinePool

func init() {
	pools = map[string]*RoutinePool{}
}

// GetRoutinePool 获取协程池，如果未创建，会自动创建并初始化
func GetRoutinePool(names ...string) *RoutinePool {
	var name string
	if len(names) == 0 {
		name = "default"
	} else {
		name = names[0]
	}
	//beego.Debug(name)
	if _, exist := pools[name]; !exist {
		pools[name] = &RoutinePool{}
		pools[name].init(name)
	}
	return pools[name]
}

func (pool *RoutinePool) GetRoutineNum() int {
	return len(pool.funcs)
}

// InitRoutinePool 初始化协程池
func InitRoutinePool(poolMaxSize int, name string, maxConcurrence ...int) {
	if _, exist := pools[name]; exist {
		return
	}
	pools[name] = &RoutinePool{}
	pools[name].MaxPoolSize = poolMaxSize
	pools[name].init(name, maxConcurrence...)
}

func (pool *RoutinePool) init(name string, maxConcurrence ...int) {
	if len(maxConcurrence) != 0 {
		pool.MaxConcurrence = maxConcurrence[0]
	} else if DefaultMaxConcurrence == 0 {
		// 如果没设最大并发数，默认20
		pool.MaxConcurrence = 20
	} else {
		pool.MaxConcurrence = DefaultMaxConcurrence
	}
	if pool.MaxPoolSize <= 0 {
		// 如果不指定协程池最大数，就默认3k，应该足够大了
		pool.MaxPoolSize = 3000
	}
	pool.funcs = make(chan func(), pool.MaxPoolSize)
	pool.lockChan = make(chan int, pool.MaxConcurrence)
	pool.Name = name
	go pool.run()
}

func (pool *RoutinePool) run() {
	for {
		select {
		case fun := <-pool.funcs:
			if fun != nil {
				pool.lockChan <- 1
				go func() {
					defer func() {
						if r := recover(); r != nil {
							logs.Error(r, string(debug.Stack()))
							<-pool.lockChan
						}
					}()
					if routineSize := len(pool.funcs); routineSize > 3000 && routineSize%1000 == 0 {
						// 协程堆积超过3k就开始警告
						logs.Warn(pool.Name, "routine pool : ", routineSize)
					}
					fun()
					<-pool.lockChan
				}()
			}
		}
	}
}

// AddRoutine 给协程池推入function
func (pool *RoutinePool) AddRoutine(goFunc func()) {
	pool.funcs <- goFunc
}

// Exit 安全退出
func (pool *RoutinePool) Exit() bool {
	if pool == nil {
		logs.Warn("RoutinePool is nil")
		return true
	}
	logs.Warn("%v 收到退出信号,等待RoutinePool处理....", pool.Name)
	stop := make(chan int, 1)
	go func() {
		//60s强制退出
		<-time.After(RoutinePoolCloseTimeOut)
		stop <- 1
	}()
	for {
		select {
		case fun := <-pool.funcs:
			if fun != nil {
				pool.lockChan <- 1
				go func() {
					defer func() {
						if r := recover(); r != nil {
							logs.Error(r, string(debug.Stack()))
							<-pool.lockChan
						}
					}()
					if routineSize := len(pool.funcs); routineSize%10 == 0 {
						// 协程退出时,每间隔10个打印一次
						logs.Warn("[Exit] routine pool: %v %v ", pool.Name, routineSize)
					}
					fun()
					<-pool.lockChan
				}()
			}
		case <-stop:
			//超时 强制退出
			//logs.Warn("RoutinePool 强制退出:%v %v ", pool.Name, len(pool.funcs))
			return false
		default:
			for {
				if len(stop) == 1 {
					//logs.Warn("RoutinePool 强制退出:%v %v ", pool.Name, len(pool.funcs))
					return false
				}
				if len(pool.lockChan) == 0 {
					logs.Warn("RoutinePool 优雅退出:%v %v ", pool.Name, len(pool.funcs))
					return true
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func Close() {
	if pools == nil || len(pools) == 0 {
		return
	}
	lock := sync.WaitGroup{}
	lock.Add(len(pools))
	for _, v := range pools {
		pool := v
		go func() {
			if !pool.Exit() {
				logs.Error("RoutinePool 强制退出:%v %v ", pool.Name, len(pool.funcs))
			}
			lock.Done()
		}()
	}
	lock.Wait()
	logs.Warn("closed :RoutinePool")
}
