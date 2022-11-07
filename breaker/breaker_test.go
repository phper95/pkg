package breaker

import (
	"testing"
	"time"
)

func TestBreaker(t *testing.T) {
	//当失败次数达到5次就会触发断路器开启，开启5秒后进入半开状态，放行2个请求，当两个请求都成功会进入关闭状态
	InitBreaker(DefaultBreaker, WithOpenStatePeriod(5*time.Second), WithBreakerCount(5), WithHalfOpenCount(2))
	for i := 0; i < 100; i++ {
		StdLogger.Println("第", i, "次请求")
		donefunc, err := GetBreaker(DefaultBreaker).Allow()
		if err != nil {
			StdLogger.Println("Allow error", err)
		}
		if donefunc != nil {
			if i > 6 {
				//标记成功
				StdLogger.Println("标记成功")
				donefunc(true)
			} else {
				//标记失败
				StdLogger.Println("标记失败")
				donefunc(false)
			}
		}

		time.Sleep(time.Second)
	}

}
