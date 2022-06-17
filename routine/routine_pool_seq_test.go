package routine

import (
	"testing"
	"time"
)

type SeqTT struct {
	N int
}

func (s *SeqTT) Incr() {
	s.N += 1
}

func TestSeqExecutor(t *testing.T) {
	timeout := time.Second * 5
	worker := 5
	seq := NewSeqExecutor(worker, 16, timeout)
	seq.Start()
	var tt = make([]SeqTT, worker)

	for i := 0; i < 1024; i++ {
		go func(n int) {
			// n = i
			seq.Put(tt[n%worker].Incr, int64(n))
		}(i)
	}
	seq.Stop()

	for i := 0; i < worker; i++ {
		routineLogger.Print("i[%v] SeqTT.N=[%v]", i, tt[i].N)
	}
	// log.Close()
}

func TestGrpool_incr(t *testing.T) {
	timeout := time.Second * 5
	worker := 5
	p := NewPool(worker, 16, timeout)
	p.Start()

	var tt = make([]SeqTT, worker)

	for i := 0; i < 1024; i++ {
		p.PutWait(tt[i%worker].Incr)
	}
	go p.Stop()
	time.Sleep(time.Second)
	for i := 0; i < worker; i++ {
		routineLogger.Print("i[%v] SeqTT.N=[%v]", i, tt[i].N)
	}

}
