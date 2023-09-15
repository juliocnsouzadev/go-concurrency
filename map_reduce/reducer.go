package map_reduce

import (
	"log"
	"sync"
)

type Reducing struct {
	workers   int
	waitGroup sync.WaitGroup
}

func NewReducer(workers int) *Reducing {
	return &Reducing{workers: workers}
}

func (r *Reducing) Reduce(countChannel chan map[string]int, wordChannels []chan string) {
	for i := 0; i < r.workers; i++ {
		r.waitGroup.Add(1)
		go func(id int) {
			r.process(id, countChannel, wordChannels)
		}(i)
	}
}

func (r *Reducing) process(reducerId int, countChannel chan map[string]int, wordChannels []chan string) {
	defer func() {
		r.waitGroup.Done()
		if reducerId == 0 {
			r.waitGroup.Wait()
			close(countChannel)
			log.Println("count channel closing")
		}
	}()
	// counting all the words seen
	localMap := make(map[string]int)
	for word := range wordChannels[reducerId] {
		localMap[word]++
	}
	countChannel <- localMap
	log.Printf("reducer %d finished\n", reducerId)
}
