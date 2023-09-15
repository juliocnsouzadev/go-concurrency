package fan_out

import (
	"context"
	"github.com/juliocnsouzadev/go-concurrency/fake"
	"github.com/juliocnsouzadev/go-concurrency/model"
	"log"
	"sync"
	"time"
)

type WorkerFunc func(worker int)

type FanOut struct {
	Workers   int
	Timeout   time.Duration
	workQueue chan model.Message
	ctx       context.Context
}

func NewFanOut(workers int, timeout time.Duration) *FanOut {
	return &FanOut{
		Workers:   workers,
		Timeout:   timeout,
		workQueue: make(chan model.Message),
		ctx:       context.Background(),
	}
}

func (f *FanOut) Process(messages []model.Message) {
	if f.Timeout.Microseconds() > 0 {
		var cancel context.CancelFunc
		f.ctx, cancel = context.WithTimeout(f.ctx, f.Timeout)
		defer cancel()
	}

	var wg sync.WaitGroup
	for i := 0; i < f.Workers; i++ { //blocks until message is received
		wg.Add(1)

		workerFunction := f.buildWorkerFunction(&wg)
		go workerFunction(i)
	}

	f.addMessageToChannel(messages) //blocks until message is available

	wg.Wait()
}

func (f *FanOut) buildWorkerFunction(wg *sync.WaitGroup) WorkerFunc {
	return func(worker int) {
		defer wg.Done()

		for msg := range f.workQueue {
			rpcMsg := &fake.RpcMessage{
				Worker: worker,
				Msg:    msg,
			}
			fake.DoRPC(rpcMsg)
		}
	}
}

func (f *FanOut) addMessageToChannel(messages []model.Message) {
	defer close(f.workQueue)
loop:
	for _, msg := range messages {
		select {
		case f.workQueue <- msg:
		case <-f.ctx.Done():
			log.Println("message sending cancelled")
			break loop
		}
	}
}
