package breaker

import (
	"testing"
	"time"
)

func TestBreaker(t *testing.T) {
	for i := 0; i < 10000; i++ {
		StdLogger.Println("第", i, "次请求")
		donefunc, err := GetBreaker(DefaultBreaker).Allow()
		if err != nil {
			StdLogger.Println("熔断器开启", err)
		}
		if donefunc != nil {
			//标记失败
			donefunc(false)
		}

		time.Sleep(time.Second)
	}

}
