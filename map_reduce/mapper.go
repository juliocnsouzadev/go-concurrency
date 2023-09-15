package map_reduce

import (
	"log"
	"strings"
	"sync"
)

type Mapping struct {
	workers   int
	waitGroup sync.WaitGroup
}

func NewMapping(workers int) *Mapping {
	return &Mapping{workers: workers}
}

func (m *Mapping) Map(lineChan *chan string, wordChannels []chan string, numReducers int) {
	for i := 0; i < m.workers; i++ {
		m.waitGroup.Add(1)
		go func(id int) {
			m.process(id, lineChan, wordChannels, numReducers)
		}(i)
	}
}

func (m *Mapping) process(mapperId int, lineChan *chan string, wordChannels []chan string, numReducers int) {
	defer func() {
		m.waitGroup.Done()
		if mapperId == 0 { // mapper with mapperId = 0 will close all reducer channels
			m.waitGroup.Wait() // wait for all mappers to conclude sending
			for i := 0; i < numReducers; i++ {
				close(wordChannels[i]) // close reducer channels.
			}
		}
	}()

	for line := range *lineChan {
		// take the first letter in the word and use it to send
		// to the correct reducer
		line = strings.ToLower(line)
		words := strings.Split(line, " ")
		for _, word := range words {
			idx := (int(word[0] - 'a')) % numReducers // dirty trick
			wordChannels[idx] <- word
		}
	}
	log.Printf("mapper %d finished\n", mapperId)
}
