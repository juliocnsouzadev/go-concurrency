package map_reduce

import (
	"log"
	"sync"
)

type MapReduce struct {
	mapper  *Mapping
	reducer *Reducing
}

func NewMapReduce(mappers, reducers int) *MapReduce {
	return &MapReduce{mapper: NewMapping(mappers), reducer: NewReducer(reducers)}
}

func (mr *MapReduce) Process(lines []string) map[string]int {

	lineChan := make(chan string)
	numReducers := mr.reducer.workers
	wordChannels := make([]chan string, numReducers)

	for i := 0; i < numReducers; i++ {
		wordChannels[i] = make(chan string)
	}
	countChannel := make(chan map[string]int)

	// mappers
	mr.mapper.Map(&lineChan, wordChannels, numReducers)

	// reducers
	mr.reducer.Reduce(countChannel, wordChannels)

	// consumer
	result, consumerWg := mr.consumer(countChannel)

	// feed the mappers each line of the file
	mr.sendLinesToChannel(lines, lineChan)

	mr.reducer.waitGroup.Wait()
	mr.mapper.waitGroup.Wait()
	consumerWg.Wait()

	return result
}

func (mr *MapReduce) sendLinesToChannel(lines []string, lineChan chan string) {
	for _, line := range lines {
		lineChan <- line
	}
	close(lineChan)
	log.Println("all lines sent!")
}

func (mr *MapReduce) consumer(countChannel chan map[string]int) (map[string]int, *sync.WaitGroup) {
	var result = make(map[string]int)
	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for counts := range countChannel {
			log.Println("consumer received: ", counts)
			for key, count := range counts {
				if _, ok := result[key]; ok {
					result[key] += count
				} else {
					result[key] = count
				}
			}
		}
		log.Println("consumer done")
	}()
	return result, &consumerWg
}
