package breaker

import (
	"github.com/sony/gobreaker"
	"log"
	"os"
	"time"
)

type breaker struct {
	breaker *gobreaker.TwoStepCircuitBreaker
}
type option struct {
	BreakerCount    uint32
	HalfOpenCount   uint32
	Interval        time.Duration
	OpenStatePeriod time.Duration
}
type Option func(*option)

const DefaultBreaker = "default"

var StdLogger stdLogger

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var breakers = make(map[string]*breaker)

func init() {
	StdLogger = log.New(os.Stdout, "[breaker] ", log.LstdFlags|log.Lshortfile)
}
func WithBreakerCount(breakerCount uint32) Option {
	return func(o *option) {
		o.BreakerCount = breakerCount
	}
}

func WithHalfOpenCount(halfOpenCount uint32) Option {
	return func(o *option) {
		o.HalfOpenCount = halfOpenCount
	}
}

func WithInterval(interval time.Duration) Option {
	return func(o *option) {
		o.Interval = interval
	}
}

func WithOpenStatePeriod(openStatePeriod time.Duration) Option {
	return func(o *option) {
		o.OpenStatePeriod = openStatePeriod
	}
}

func InitBreaker(breakerName string, options ...Option) *breaker {
	var b *breaker

	if b, ok := breakers[breakerName]; ok {
		return b
	} else {
		b = &breaker{}
		opt := &option{}
		halfOpenCount := uint32(2)
		breakCount := uint32(20)
		interval := 5 * time.Minute
		openStatePeriod := 3 * time.Minute
		for _, f := range options {
			f(opt)
		}
		if opt.BreakerCount > 0 {
			breakCount = opt.BreakerCount
		}

		if opt.HalfOpenCount > 0 {
			halfOpenCount = opt.HalfOpenCount
		}

		if opt.Interval > 0 {
			interval = opt.Interval
		}

		cb := gobreaker.NewTwoStepCircuitBreaker(gobreaker.Settings{
			Name:        breakerName,
			MaxRequests: halfOpenCount,
			Interval:    interval,
			Timeout:     openStatePeriod,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				result := counts.ConsecutiveFailures > breakCount
				if result {
					StdLogger.Printf("[%s] CircuitBreaker ConsecutiveFailures %d", breakerName, counts.ConsecutiveFailures)
				}
				return result
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				StdLogger.Printf("[%s] CircuitBreaker state change from[%s] -> to[%s]", name, from.String(), to.String())
			},
		})
		b.breaker = cb
		breakers[breakerName] = b
		StdLogger.Printf("Create CircuitBreaker name : %s ; breakCount : %d ; halfOpenCount : %d ; interval %v ;"+
			" openStatePeriod %v", breakerName, breakCount, halfOpenCount, interval, openStatePeriod)
	}
	return b
}

func GetBreaker(breakerName string) *gobreaker.TwoStepCircuitBreaker {
	if b, ok := breakers[breakerName]; ok {
		return b.breaker
	} else {
		panic("please call InitBreaker before !!! ")
	}
}
