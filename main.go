package main

import (
	"fmt"
	"github.com/juliocnsouzadev/go-concurrency/fan_out"
	"github.com/juliocnsouzadev/go-concurrency/model"
	"time"
)

func main() {
	fmt.Println("\n***********************************\n\n")
	numberOfMessages := 1000
	numberOfWorkers := 5

	messages := make([]model.Message, numberOfMessages)
	for i := 0; i < numberOfMessages; i++ {
		messages[i] = model.Message{
			Id:   i + 1,
			Body: fmt.Sprintf("Message #%d", i+1),
		}
	}

	fanOut := fan_out.NewFanOut(numberOfWorkers, 10*time.Second)
	fanOut.Process(messages)
}
